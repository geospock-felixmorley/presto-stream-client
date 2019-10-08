const http = require('http');
const https = require('https');
const { Readable } = require('stream');
//const { URL } = require('url');
const { Headers } = require('./headers');
const VERSION = require('../package.json').version;
// symbols below used to hide properties considered private
const s_objectMode = Symbol('Object Mode');
const s_SOF = Symbol('Start of File');
const s_EOF = Symbol('End of File');
const s_id = Symbol("Query ID");
const s_cancelled = Symbol("Cancelled Status");
const s_options = Symbol("Cached request options");
const s_state = Symbol("Query state (as far as client knows)");
const s_nextUri = Symbol("The next URI to retrieve");
const s_request = Symbol("Client request");
const s_requestPromise = Symbol("Client request, promisified");
const s_session = Symbol("Session Data");
const s_ssl  = Symbol("SSL info");
const s_isRunning = Symbol("Run in progress");

const QUERY_STATE_CHECK_INTERVAL = 3000; // in ms
class prestoError extends Error {
    constructor(message,props) {
        super(message);
        this.name = 'prestoError';
        if(props instanceof Object && props.hasOwnProperty('data')) {
            this.data = props.data;
        }
        if(props instanceof Object && props.hasOwnProperty('response_code')) {
            this.response_code = props.response_code;
            this.name += `: code ${props.response_code}`;
        }
        if(props instanceof Object && props.hasOwnProperty('response_type')) {
            this.response_type = props.response_type;
        }
    }

}

function convertToCSV(input){
    switch (typeof input){
        case 'number':
            return input;
        case 'object':
            if(input === null){
                return `""`;
            }
            //fallthrough
        default:
            return `"${String(input).replace('"','""')}"`;
    }
}
/**
 * @description helper function make a generic http / https request to presto
 * Ideally, this would be async but async code seems to conflict with stream.Readable's requirement to emit errors (causing this.emit to reject the promise as side effect)
 * in any case this function is not exposed so external api can still be promisified
 */
function makeRequest(opts,contentBody,useSSL,jsonParser,callback) {
    opts.agent = (useSSL) ? new https.Agent(opts) : new http.Agent(opts);
    new Promise((resolve,reject)=>{
        const req = (useSSL) ? https.request(opts) : http.request(opts);
        //buffer the entire response before handing off to next step (required since response will be JSON)
        req.on('response',(res)=>{
            res.setEncoding('utf8');
            const response_code = res.statusCode;
            const response_data = [];
            res.on('data', (chunk)=>{
                response_data.push(chunk);
            }).on('end', ()=>{
                return resolve({ response_code,
                    data:response_data.join('') ,
                    response_type: res.headers['content-type'] ,
                    session: res.headers[Headers.SET_SESSION] ,
                    clear_session:res.headers[Headers.CLEAR_SESSION]
                });
            });
        }).on('error', function(e){
            return reject(e);
        });
        if (contentBody) {req.write(contentBody);}
        req.end();
    }).then((response)=>{
        if (response.code === 503 ) { // https://github.com/prestodb/presto/wiki/HTTP-Protocol "If the response is an HTTP 503, sleep 50-100ms and try again"
            setTimeout(()=>{
                return makeRequest(opts,contentBody,useSSL,jsonParser,callback);
            }, Math.floor(Math.random() * (51)) + 50); //randomly sleep between 50 and 100 ms so concurrent requests are spread out
        } else if(response.response_code < 300) {
            if(response.response_type !== 'application/json' || response.data.length < 2) { //some apis such as DELETE do not have a body
                return callback(null,{ response_code : response.response_code , data: {}});
            }
            try {
                const data = jsonParser.parse(response.data);
                const output = {response_code:response.response_code, data};
                if(response.data.updateType === true) {
                //SET SESSION or RESET SESSION https://github.com/prestodb/presto/wiki/HTTP-Protocol
                    if(response.session) {
                        output.session = response.session;
                    } else {
                        output.clear_session = true;
                    }
                }
                return callback(null,output);
            } catch (x) {
            /** presto with response type application/json should always return JSON if not treat as failure https://github.com/prestodb/presto/wiki/HTTP-Protocol*/
                return callback(new prestoError('request failed: unintelligible response.',response));
            }
        } else {
            return callback(new prestoError('invalid response code',response));
        }
    },(error)=>{
        return callback(error);
    });
}
/**
 * @description a presto client.
 * @param {Object} args - host,port,user,password, catalog, schema, source, pollInterval, jsonParser , objectMode , ssl
 */
class Client {
    /**
     * @description constructor for class Client, a presto client.
     * @param {Object} args (optional)
     * host: presto coordinator address, default: localhost
     * port: presto coordinator port, default: 8080
     * ssl: if an object is provided, activate SSL with any extra settings as defined in the object. default: no ssl
     * user: query user, default: current user (process.env.USER),
     * password: if provided, authenticate with Basic Auth.
     * catalog: default presto catalog. default: hive.
     * schema: default schema. default: default.
     * source: query source, default nodejs-client.
     * pollInterval: status polling interval while waiting for query to finish. default: 3000 ms.
     * jsonParser: custom parser for JSON (in case bigint support needed ). default: JSON.
     * objectMode: default mode for statements, default: false. (e.g. statements will by default return a CSV readstream)
     */
    constructor(args = {}){
        this[s_options] = {headers:{}};
        this[s_options].host = args.host || 'localhost';
        this[s_options].port  = args.port || 8080;
        if(args.ssl && args.ssl instanceof Object) {
            Object.keys(args.ssl).forEach((key)=>{
                this[s_options][key] = args.ssl[key];
            });
            this[s_ssl] = Object.freeze(Object.assign({},args.ssl));
        } else {
            this[s_ssl] = false;
        }
        this.catalog = args.catalog || 'hive';
        this.schema = args.schema || 'default';
        this.pollInterval = args.pollInterval || QUERY_STATE_CHECK_INTERVAL;
        this.jsonParser = args.jsonParser || JSON;
        this.objectMode = (args.hasOwnProperty('objectMode') && args.objectMode) ? true : false;
        this[s_options].headers[Headers.USER_AGENT] = 'presto-stream-client-' + VERSION;
        this[s_options].headers[Headers.SOURCE] = args.source || 'nodejs-client';
        this[s_options].headers[Headers.USER] = args.user || process.env.USER;
        // Apply an Authorization header if the user has specified a password.
        if (args.password){
            this[s_options].headers[Headers.AUTHORIZATION] = 'Basic ' + Buffer.from(this[s_options].headers[Headers.USER] + ":" + args.password).toString("base64");
        }
        // internal call to makeRequest, given options setup by this client
        this[s_request] = (opts,callback)=>{
            const contentBody = (opts.body) ? opts.body : null;
            const requestOpts = Object.assign({},this[s_options]); //copy options by value
            if(opts.headers) { //headers is child object, copy by value, override if provided
                requestOpts.headers = Object.assign({},this[s_options].headers,opts.headers);
            }
            if (opts.session) {
                // https://github.com/prestodb/presto/wiki/HTTP-Protocol Statements submitted following SET SESSION statements should include any key-value pairs (returned by the servers X-Presto-Set-Session) in the header X-Presto-Session. Multiple pairs can be comma-separated and included in a single header.
                requestOpts.header[Headers.SESSION] = opts.session;
            }
            for(const key of Object.keys(opts)){
                if(key !== 'headers') {
                    requestOpts[key] = opts[key];
                }
            }
            return makeRequest(requestOpts,contentBody,this[s_ssl],this.jsonParser,callback);
        };
        // promisified call to makeRequest to simplify code where async is used
        this[s_requestPromise] = (opts)=>{
            return new Promise((resolve,reject)=>{
                this[s_request](opts,(error,response)=>{
                    if(error){
                        return reject(error);
                    } else {
                        return resolve(response);
                    }
                });
            });
        };

    }

    get ssl() {
        return this[s_ssl];
    }
    /**
     * @description return a list of active or failed nodes (GET /v1/node or GET /v1/node/failed)
     * @param active {boolean} - whether to retrieve all known nodes or only currently failing nodes. Default: all known.
     * docs: https://github.com/prestodb/presto/blob/master/presto-docs/src/main/sphinx/rest/node.rst
     */
    async nodes(failed = false) {
        const path = '/v1/node' + ((failed) ? '/failed' : '');
        const { data } = await this[s_requestPromise]({ method: 'GET', path });
        return data;
    }
    /**
     * @description returns status of the presto cluster
     */
    async cluster() {
        const { data } = await this[s_requestPromise]({ method: 'GET' , path: '/v1/cluster'});
        return data;
    }

    /**
     * @description returns current status of the provided query
     * @param query_id {string} the ID of the desired query
     */
    async status(query_id) {
        const { data } = await this[s_requestPromise]({ method: 'GET', path: '/v1/query/' + query_id });
        return data;
    }
    /**
     * @description cancels a currently running query. Note, statement.cancel will more gracefully exit if a stream is actively getting data.
     * @param query_id {string} the ID of the desired query
     */
    async kill(query_id) {
        await this[s_requestPromise]({ method: 'DELETE', path: '/v1/query/' + query_id });
        return;
    }
    /**
     * @description execute a query on the presto cluster.
     * @param {Object} opts - properties:
     * catalog: the catalog to run the statement against (default, client catalog)
     * schema: the schema to run the statement against (default, client schema)
     * query: the query to run on presto. (required)
     * timezone: the timezone to run the query in.
     * session: the session string provided by Presto to consider this statement a part of a session
     * objectMode: whether the statement will run in Object Mode or not. If true, will be a stream of objects. If false, will be a stream of CSV strings.
     * highWaterMark: the highWaterMark for the statement stream. (exactly as per stream.Readable)
     * @returns Statement object
     */
    async execute(opts){
        if (!opts.catalog && !this.catalog) {
            throw new Error("catalog not specified");
        } else if (!opts.schema && !this.schema) {
            throw new Error("schema not specified");
        } else if (!opts.query || !(typeof opts.query === 'string')) {
            throw new Error('query not specified or is invalid');
        }
        const header = {};
        header[Headers.CATALOG] = opts.catalog || this.catalog;
        header[Headers.SCHEMA] = opts.schema || this.schema;
        if (opts.timezone) {header[Headers.TIME_ZONE] = opts.timezone;}
        if (opts.session) {
            // https://github.com/prestodb/presto/wiki/HTTP-Protocol Statements submitted following SET SESSION statements should include any key-value pairs (returned by the servers X-Presto-Set-Session) in the header X-Presto-Session. Multiple pairs can be comma-separated and included in a single header.
            opts.header[Headers.SESSION] = opts.session;
        }

        const { response_code , data , session } = await this[s_requestPromise]({ method: 'POST', path: '/v1/statement', headers: header, body: opts.query });

        if(response_code !== 200 || data && data.error) {
            if (data.error.message) {
                throw new prestoError(data.error.message,{ response_code });
            } else {
                throw new prestoError("execution error" + (data && data.length > 0 ? ":" + data : ""),{ response_code });
            }
        } else if (!data.id) {
            throw new prestoError("query id missing in response for POST /v1/statement", { data });
        } else if (!data.nextUri){
            throw new prestoError("nextUri missing in response for POST /v1/statement", { data });
        } else if (!data.infoUri) {
            throw new prestoError("infoUri missing in response for POST /v1/statement", { data });
        }
        const streamOpts = {};
        streamOpts.objectMode = opts.hasOwnProperty('objectMode') ? opts.objectMode : this.objectMode;
        if(opts.highWaterMark) { streamOpts.highWaterMark = opts.highWaterMark; }
        /*
    var data = {
      "stats": {
        "processedBytes": 0, "processedRows": 0,
        "wallTimeMillis": 0, "cpuTimeMillis": 0, "userTimeMillis": 0,
        "state": "QUEUED",
        "scheduled": false,
        "nodes": 0,
        "totalSplits": 0, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 0,
      },
      "nextUri": "http://localhost:8080/v1/statement/20140120_032523_00000_32v8g/1",
      "infoUri": "http://localhost:8080/v1/query/20140120_032523_00000_32v8g",
      "id": "20140120_032523_00000_32v8g"
    };
     */
        return new Statement(streamOpts,this,data.nextUri,data.id,opts.info || false,this.pollInterval,session);
    }
}

class Statement extends Readable {
    /**
     * @description constructor for Statement class, used to manage a single execution of a Presto query.
     * @param {Object} readableOptions - optional parameters highWaterMark, objectMode. (other Readable options are fixed)
     * @param {Client} client - related presto client
     * @param {String} initialUri - the first uri to fetch data from (provided by client)
     * @param {String} queryid - the query ID
     * @param {Boolean} fetchInfo - whether to retrieve Info on success event or not
     * @param {Number} pollInterval - milliseconds to poll for state changes
     * @param {String} session - related session data
     */
    constructor(readableOptions,client,initialUri,queryId,fetchInfo = false,pollInterval = QUERY_STATE_CHECK_INTERVAL,session = null){
        const opts = {objectMode:false,autoDestroy:true};
        if(readableOptions.highWaterMark) {
            opts.highWaterMark = readableOptions.highWaterMark;
        }
        if(readableOptions.hasOwnProperty('objectMode') && readableOptions.objectMode) {
            opts.objectMode = true;
        } else {
            opts.encoding = 'utf8';
        }
        super(opts);
        this[s_objectMode] = opts.objectMode; //cannot use this before super
        this[s_EOF] = false;
        this[s_SOF] = false;
        this[s_id] = queryId;
        this[s_state] = null;
        this[s_nextUri] = initialUri;
        this[s_session] = session || null;
        this[s_cancelled] = false;
        this[s_isRunning] = false;
        this.client = client;
        this.columns = null;
        this.fetchInfo = (fetchInfo) ? true : false;
    }
    /**
     * @description returns the query ID related to this statement
     */
    get query_id() {
        return this[s_id];
    }
    /**
     * @description get the currrent state of the query.
     */
    get state() {
        return this[s_state];
    }
    /**
     * @description get the session for re-use
     */
    get session() {
        return this[s_session];
    }
    /**
     * @description cancel the running query.
     */
    async cancel() {
        this[s_cancelled] = true; //flag to stop
        this[s_nextUri] = null;
        return await this.client.kill(this[s_id]);
    }
    /**
     * @description Internal, required as a Readable implementation
     */
    _read() {
        if (!this[s_isRunning]) {
            this._run();
        }
    }
    /**
     * @description Internal, required as a Readable implementation
     */
    _destroy(error,callback) {
        if(!['FINISHED', 'CANCELED', 'FAILED'].includes(this[s_state])) {
            //cancel query if it is still running on server
            this.cancel().then(()=>{
                this.client = null;
                return callback(null);
            },(err)=>{
                this.client = null;
                return callback(err);
            });
        } else {
            this[s_nextUri] = null;
            this.client = null;
        }
    }

    _statementCancelled(){
        if(this[s_cancelled]) { //check if cancelled before doing anything else.
            if(!this[s_EOF]) { //if not yet reached EOF push null to signal EOF
                this[s_EOF] = true;
                this.push(null);
            }
            return true;  //notify to  end run if cancelled
        }
        return false;
    }
    /**
     * @description. Internal. fetches packets of data from Presto
     */
    _run() {
        this[s_isRunning] = true;
        const requestOpts = { path: this[s_nextUri] };
        if(this[s_session]) {
            requestOpts.session = this[s_session];
        }
        if(this._statementCancelled()){
            this[s_isRunning] = false;
            return; //check before request to presto to avoid an unnecessary call to presto server
        }
        this.client[s_request](requestOpts,(err, responseOutput)=>{
            if(err) {
                return this.emit('error',err);
            }

            let { response_code , data , session , clear_session} = responseOutput;

            if(this._statementCancelled()){
                this[s_isRunning] = false;
                return; //check again after request since request runs async and a call to cancel may have occured in the interim
            }
            if(session){
                this[s_session] = session;
            } else if (clear_session) {
                this[s_session] = null;
            }
            /*
            * 1st time
                {
                "stats": {
                "rootStage": {
                    "subStages": [
                    {
                        "subStages": [],
                        "processedBytes": 83103149, "processedRows": 2532704,
                        "wallTimeMillis": 20502, "cpuTimeMillis": 3431, "userTimeMillis": 3210,
                        "stageId": "1", "state": "FINISHED", "done": true,
                        "nodes": 3,
                        "totalSplits": 420, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 420
                    }
                    ],
                    // same as substage
                },
                // same as substage
                "state": "RUNNING",
                },
                "data": [ [ 1266352 ] ],
                "columns": [ { "type": "bigint", "name": "cnt" } ],
                "nextUri": "http://localhost:8080/v1/statement/20140120_032523_00000_32v8g/2",
                "partialCancelUri": "http://10.0.0.0:8080/v1/stage/20140120_032523_00000_32v8g.0",
                "infoUri": "http://localhost:8080/v1/query/20140120_032523_00000_32v8g",
                "id": "20140120_032523_00000_32v8g"
                }
            * 2nd time
                    {
                    "stats": {
                    // ....
                    "state": "FINISHED",
                    },
                    "columns": [ { "type": "bigint", "name": "cnt" } ],
                    "infoUri": "http://localhost:8080/v1/query/20140120_032523_00000_32v8g",
                    "id": "20140120_032523_00000_32v8g"
                    }
            */
            if(data.error) {
                if (data.error.message) {
                    this.emit('error',new prestoError(data.error.message,{ response_code , data }));
                } else {
                    this.emit('error',new prestoError('attempt to retrieve next dataUri failed',{ response_code, data }));
                }
                //end run if error, and attempt to cancel on Presto as well
                this.cancel().catch((error)=>{ // if call to cancel errors, emit that error
                    this.emit('error',error);
                });
                this[s_isRunning] = false;
                return;
            }

            if(this.listenerCount('state') > 0 && !this[s_SOF]) { //only emit state event if something is listening & file has not started downloading
                this.emit('state',data.stats.state,data.stats);
            }
            if(this[s_state] !== data.stats.state) {
                this[s_state] = data.stats.state;
                this.emit('state_change',data.stats.state,data.stats);
                if(this[s_state] === 'FINISHED') {
                    this[s_SOF] = true;
                }
            }
            /* presto-main/src/main/java/com/facebook/presto/execution/QueryState.java
            * QUEUED, PLANNING, STARTING, RUNNING, FINISHED, CANCELED, FAILED
            */
            if ((data.stats.state === 'QUEUED' || data.stats.state === 'PLANNING'
                || data.stats.state === 'STARTING' || data.stats.state === 'RUNNING')
            && !data.data) {
                this[s_nextUri] = data.nextUri;
                //currently waiting for the query to finish. wait pollInterval frequency and run again.
                return setTimeout(()=>{ this._run(); },this.pollInterval);
            }
            let canPush = true; //keep track whether downstream can receive data
            if(!this.columns && data.columns) {
                this.columns = data.columns;
                this.emit('columns',data.columns);
                if(this[s_objectMode]) { //object mode, guarantee uniqueness of column names
                    const uniqueColumns = new Set(); const duplicates = {};
                    this.columns.forEach((column)=>{
                        if(uniqueColumns.has(column.name)) {
                            duplicates[column.name] = 1;
                        } else {
                            uniqueColumns.add(column.name);
                        }
                    });
                    if(Object.keys(duplicates).length > 0) { //only run if duplicates detected
                        for (let i = 0; i < this.columns.length; i++) {
                            const colName = this.columns[i].name;
                            if(duplicates.hasOwnProperty(colName)) {
                                this.columns[i].name += '_' + duplicates[colName];
                                duplicates[colName]++;
                            }
                        }
                    }
                } else { //not object mode, push out CSV header
                    canPush = this.push(this.columns.map(i=>`"${String(i.name).replace('"','""')}"`).join(',') + '\n');
                }
            }
            if (data.data) {
                if(this[s_objectMode]) {  //if object mode, push out object in form column name: value
                    //push data received in this request row by row as objects
                    //since data is received in bulk it has to be buffered in memory one way or another, so push it all
                    // however keep track whether canPush more, to either stop before next request or keep going immediately
                    for (let i = 0; i < data.data.length; i++) {
                        const output = {};
                        for(let y = 0; y < this.columns.length; y++) {
                            output[this.columns[y].name] = data.data[i][y];
                        }
                        canPush = this.push(output);
                    }
                } else { //when data is being sent in string mode, send the entire response data as a string in one push
                    canPush = this.push(data.data.map(i => i.map(convertToCSV).join(',')).join('\n'));
                }
            }
            if(data.nextUri) {
                this[s_nextUri] = data.nextUri;
                if(canPush) { //if not too much pressure on writeStream, get the next value without waiting.
                    return this._run();
                } else {
                    this[s_isRunning] = false;
                    return;
                }
            } else {
                //if no nextUri, reached end of datastream.
                // if not already done (from cancellation) push null to signal EOF.
                if(!this[s_EOF]) {
                    this[s_EOF] = true;
                    this.push(null);
                }
                if(this.fetchInfo && data.infoUri) {
                    //const {hostname:ihost , iport, pathname:ipath} = new URL(data.infoUri);
                    //const { data : info } = await this.client.request({host:ihost , port:iport , path:ipath });
                    this.client[s_request]({ path : data.infoUri}).then(({ data : info })=>{
                        this.emit('success',{ stats: data.stats , info });
                    },(error)=>{
                        this.emit('success',{stats: data.stats ,info:{error:error}});
                    });
                } else {
                    this.emit('success',data.stats);
                }
                this[s_isRunning] = false;
                return;
            }
        });
    }
}

module.exports = { Client , Statement , VERSION };
