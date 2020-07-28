package service

import com.alibaba.fastjson.JSONObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexOptions
import com.mongodb.util.JSON
import org.bson.Document
import org.bson.types.ObjectId

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

/**********mongodb操作***********/
/*
 *  带有inner的方法不可被外部调用!
 *  saveAll类名             saveAllUser([]) 保存一个list list中每条记录需要手动设置_id
 *  save类名             saveUser([:]) 保存单map数据
 *  update类名         updateUser([:],[:]) 第一个参数是查询条件。第二个是要改的内容，本方法只更新1条记录
 *  updateAll类名    updateAllUser([:],[:]) 第一个参数是查询条件。第二个是要改的内容，本方法更新所有符合条件的记录，要谨慎
 *  del类名               delUser([:]) 参数值为查询条件
 *  distinct类名        distinctCart("agentId",[phoneId:"12345"])第1个参数为group的key，第2个参数是查询条件
 *                      distinctCart("agentId",[phoneId:"12345"],String)第1个参数为group的key，第2个参数是查询条件 第3个参数是查询字段的类型
 *  search类名         searchUser([:],page,maxResult,getCount,[include:[],exclude:[]])第一个参数为查询条件(可以有sort参数)，第二、三、四、五个参数可选
 *  findOneAndModify类名  findAndModifyUser([_id:'111'],['\$set':[name:123]]),每次操作一条数据,set参数可写可不写,其他参数如unset、inc等命令,需要自己写. 此方法具有事务性，可用于系统中订单号的生成
 *  findOneAndReplace类名  findAndModifyUser(query,replace_data,[sort]) sort参数可选
 *  findOne类名       findOneUser([:],[include:[],exclude:[]]) 第一个参数是查询条件(可以有sort参数)
 *  			      第二个参数extParams中有两个属性include和exclude，代表要包括哪些字段，或不要哪些字段。这两个属性只能二选一。如果都写，则只算exclude
 *                            例如： findUser([name:"abc"],[exclude:['photo']])
 *  find类名             findUser([:],[include:[],exclude:[]]) 第一个参数是查询条件,条件中可包括sort及limit参数,比如findUser([name:"张三",sort:[ct:-1],limit:50])
 *                            第二个参数extParams中有两个属性include和exclude，代表要包括哪些字段，或不要哪些字段。这两个属性只能二选一。如果都写，则只算exclude
 *                            例如： findUser([name:"abc"],[exclude:['photo']])
 *  findCount类名   findCountUser([:]) 根据条件得到符合的记录数 返回int
 *  findSum类名   findSumMoneyItem([agentId:111],["money","fanliMoney"]) 第1个参数是查询条件，第2个参数是数组，
 *  表明要sum哪些字段。注意并不是返回money+fanliMoney，而是返回sum(money)，和sum(fanliMoney)两个字段
 *  callBackInFind类名  callBackInFindUser([:],callBack) 根据条件得到符合记录数 把记录传给callBack处理 callBack(item),query里可包括sort及limit参数
 *  dropTable表名  删除一张表，此方法慎用
 */

class DbService {
    //以下为数据库连接配置,可以考虑用spring注入参数
    def mongoClient
    def dataBaseName
    def db
    def dbVersion = "4.0.19";//mongo版本号 默认4.0

    /*
	 *  根据构造函数来建立
	 *  ip   		must	服务器ip 集群的情况下,不能使用此方式
	 *  port  		must	服务器端口  集群的情况下,不能使用此方式
	 *  hosts [ip1:port1,ip2:port2...]   集群的情况下使用
	 *  dbName  		must	数据库名
	 *  mongoUser  		must	登录名
	 *  mongoUserPwd  	must	登录密码
	 *  connectTimeout  option  default 60000 60秒 连接超时时间
	 *  socketTimeout   option  default 60000 60秒 数据读取超时时间
	 *  maxPoolSize		option  default 800
	 *  minPoolSize		option  default 50
	 *  authSource		option  用户名密码效验的库名
	 *  maxWaitTime     option 等待连接阻塞时间 1秒
	 *  version         option 当前使用的数据库的版本号
	 */
//    连接配置说明:
//    http://mongodb.github.io/mongo-java-driver/4.0/apidocs/mongodb-driver-core/com/mongodb/ConnectionString.html
//    https://mongodb.github.io/mongo-java-driver/3.12/javadoc/com/mongodb/ConnectionString.html
    public DbService(Map params){
        this.dataBaseName = params.dbName
        String sourceDb = dataBaseName
        if(params.authSource) {
            sourceDb = params.authSource;
        }
        int connectTimeout =  60000;
        if(params.connectTimeout) {
            connectTimeout = params.connectTimeout.toString().toInteger();
        }

        int socketTimeout =  60000;
        if(params.socketTimeout) {
            socketTimeout = params.socketTimeout.toString().toInteger();
        }

        int maxPoolSize =  200;
        if(params.maxPoolSize) {
            maxPoolSize = params.maxPoolSize.toString().toInteger();
        }
        int minPoolSize =  20;
        if(params.minPoolSize) {
            minPoolSize = params.minPoolSize.toString().toInteger();
        }

        if(maxPoolSize<minPoolSize){
            maxPoolSize = maxPoolSize;
        }

        int maxWaitTime = 1000;
        if(params.maxWaitTime) {
            maxWaitTime = params.maxWaitTime.toString().toInteger();
        }
        String version = ""
        if(params.version) {
            version = params.version.toString();
        }
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.maxWaitTime(maxWaitTime)  //最大等待连接的线程阻塞时间 毫秒
        .minConnectionsPerHost(minPoolSize) //最小连接数
                .connectionsPerHost(maxPoolSize) //最大连接数
//              .threadsAllowedToBlockForConnectionMultiplier(10) //线程队列数，它与connectionsPerHost值相乘的结果就是线程队列最大值
//              .threadsAllowedToBlockForConnectionMultiplier(5) //超过此值乘以connectionsPerHost,将返回异常
//              .socketKeepAlive(false) //设置keepalive,默认值是false
                .connectTimeout(connectTimeout) //连接超时时间
                .socketTimeout(socketTimeout); //socket超时

        String ip = "localhost"
        int port = 27017
        if(params.ip){
            ip = params.ip.toString();
        }
        if(params.port){
            port = params.port.toString().toInteger();
        }
        /*
        需要效验所有参数的合法性
        此处暂省略...
         */
        MongoClientOptions clientOptions = builder.build();
        this.mongoClient = new MongoClient(new ServerAddress(ip,port),
                Arrays.asList(MongoCredential.createCredential(params.mongoUser, sourceDb, params.mongoUserPwd.toCharArray())),clientOptions);
        this.db = this.mongoClient.getDatabase(dataBaseName);
        if(!version){
            version = db.runCommand(new Document("buildInfo",1)).get("version")
        }
        if(version){
            this.dbVersion = version.trim()
        }
    }

    def methodMissing(String name, args) {
        if (name.startsWith("saveAll")) {
            def now = new Date()
            def condition = args[0]
            if (args[0] instanceof Collection) {
                condition = mapToBson(args[0])
            }else{
                return  [:]
            }
            try{
                condition.each {
                    Document d = it
                    if (!d["_id"]) {//id
                        d["_id"] = new ObjectId().toHexString();
                    }
                }
                innerGetColl(name,7).insertMany(condition)
            }catch(e){
                e.printStackTrace();
                throw e;
                return  [:]
            }
        } else if (name.startsWith("save")) {
            def now = new Date()
            def condition = args[0]

            if (!condition.ct) {//createTime
                def dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                condition.ct = dateformat.format(now);
            }
            if (!condition["_id"]) {//id
                condition["_id"] = new ObjectId().toHexString();
            }
            args[0]["_id"] = condition["_id"] //相当于把保存后的主键id返回
            args[0]["ct"] = condition["ct"] //相当于把保存后的ct返回
            condition = mapToBson(args[0])
            try{
                innerGetColl(name, 4).insertOne(condition)
            }catch(e){
                e.printStackTrace();
                throw e;
                return  [:]
            }
            return args[0]
        }else if (name.startsWith("updateAll")) {
            def query = [:]
            if(args){
                query = args[0]==null?[:]:mapToBson(args[0])
            }
//            def modify = args[1]
            def noSet = [:]//map中的key值不受到$set影响
            def modify = innerTranslateMap(args[1], noSet)
            //使用set修改器
            def tmpId = modify._id
            modify.remove("_id") //不要_id
            def all = [:]
            if (modify&&!modify.containsKey("\$set")) all = ["\$set": modify]  //modify不为空,且没有设置$set操作时,设置一下
            all.putAll(noSet)//把noSet值拷入
            modify = mapToBson(all)
            try {
                innerGetColl(name, 9).updateMany(query, modify)
            } catch (e) {
                e.printStackTrace();
                throw e;
            }
//			if (tmpId) {
//				modify._id = tmpId //再设置回来
//			}
            return  [:]
        } else if (name.startsWith("update")) {
            //https://docs.mongodb.com/manual/reference/operator/update-field/
            def query = [:]
            if(args){
                query = args[0]==null?[:]:mapToBson(args[0])
            }
//			println query
//			println modify
            def objBack = args[1] //为了存储_id
            def noSet = [:]//map中的key值不受到$set影响
            def modify = innerTranslateMap(args[1], noSet)
            //使用set修改器
            def tmpId = modify._id
            modify.remove("_id") //不要_id
            def all = [:]
            if (modify&&!modify.containsKey("\$set")) all = ["\$set": modify]  //modify不为空,且没有设置$set操作时,设置一下
            //需要做判断,不然会报'$set' is empty. You must specify a field like so: {$mod: {<field>: ...}}
            all.putAll(noSet)//把noSet值拷入
            modify = mapToBson(all)
            try {
                innerGetColl(name, 6).updateOne(query, modify)//最后两个参数是upset和multiLine
            } catch (e) {
                e.printStackTrace();
                throw e;
            }
            if (tmpId) {
                objBack._id = tmpId //再设置回来
            }
            return  [:]
        } else if (name.startsWith("distinct")) {
            def coll = innerGetColl(name, 8)
            def keytemp = args[0].toString()
            def query = [:]
            if(args){
                query = args[1]==null?[:]:args[1]
            }
            def result_class = String//返回的数据类型,如:String
            if (args.size() >= 3) {
                result_class = args[2]
            }
            def list = []
            if(query){
                coll.distinct(keytemp, mapToBson(query), result_class).into(list)
            }else{
                coll.distinct(keytemp,result_class).into(list)
            }

            //第二种方式
//			coll.distinct(keytemp, query, result_class).iterator().each {
////				it.remove("_id")
//				list << it;
//			}
            return list;
        } else if (name.startsWith("findOneAndModify")) {//每次只能更新一条记录
            def coll = innerGetColl(name, 16)
            def query = [:]
            if(args){
                query = args[0]==null?[:]:mapToBson(args[0])
            }
            def noSet = [:]//map中的key值不受到$set影响
            def modify = innerTranslateMap(args[1], noSet)
            def tmpId = modify._id
            modify.remove("_id") //不要_id

            def all = [:]
            if (modify&&!modify.containsKey("\$set")) all = ["\$set": modify]  //modify不为空,且没有设置$set操作时,设置一下
            all.putAll(noSet)//把noSet值拷入
            modify = mapToBson(all)

            def sort = [_id: 1]
            if(args.size()>2){
                sort = args[2]
            }

            FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
            options.sort(mapToBson(sort))
            def ret = coll.findOneAndUpdate(query, modify,options)

            return ret

        } else if (name.startsWith("findOneAndReplace")) {//每次只能替换一条记录
            def coll = innerGetColl(name, 17)
            def query = [:]
            if(args){
                query = args[0]==null?[:]:mapToBson(args[0])
            }
            def replace_data = mapToBson(args[1])
            def sort = [_id:-1]
            if(args.size()>2){
                sort = args[2]
            }
            sort =  mapToBson(args[2])
            FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
            options.sort(sort);
            options.upsert(true)
            def ret = coll.findOneAndReplace(query, replace_data,options)
            return ret
        }else if (name.startsWith("findOne")) {
            def coll = innerGetColl(name, 7)
            def query = [:]
            def extPara = null
            if(args){
                query = args[0]==null?[:]:mapToBson(args[0])
                extPara = args.size() == 2 ? args[1] : null
            }

            def pagebean = innerSearchRecord(coll, query, 1, 1, false, normalCallBack, extPara)

            if (pagebean.contentlist.size() > 0)
                return pagebean.contentlist[0]
            else
                return null
        } else if (name.startsWith("findCount")) {
            def coll = innerGetColl(name, 9)
            def query = [:]
            if(args){
                query = args[0]==null?[:]:args[0]
            }
            query = mapToBson(query)
            def ret =  coll.count(query)
//            def ret =  coll.countDocuments(query) //新版mongo驱动中使用
            return ret
        } else if (name.startsWith("findSum")) {
            if (args.size() != 2) return null
            def coll = innerGetColl(name, 7)
            def project = [:], group = [_id: 1]
            args[1].each {
                project.put(it, 1)
                group.put(it, ["\$sum": "\$" + it + ""])
            }
            def pipelineList = []
            pipelineList << mapToBson(["\$match": args[0]])
            pipelineList << mapToBson(["\$project": project])
            pipelineList << mapToBson(["\$group": group])
//			def ret= coll.aggregate(*pipelineList);
            def ret = []
            coll.aggregate(pipelineList).into(ret);

            if (ret?.size() > 0) {
                def result = ret.iterator().next();
                result.remove("_id")//不要id
                return result
            } else {
                return [:]
            }
        } else if (name.startsWith("find")) {
            def coll = innerGetColl(name, 4)
            def query = [:]
            def extPara = null
            if(args){
                query = args[0]==null?[:]:mapToBson(args[0])
                extPara = args.size() == 2 ? args[1] : null
            }

            def pagebean = innerSearchRecord(coll, query, 1, 1000, false, normalCallBack, extPara)//最大每次查1000条

            return pagebean.contentlist
        } else if (name.startsWith("search")) {
            //参数顺序(兼容老系统):
            //1.query page maxResult getCount=true callback extPara
            //2.query extPara page maxResult getCount=true
            args = args as List
            def coll = innerGetColl(name, 6)
            def query = [:]
            if(args){
                query = args[0]==null?[:]:args[0].asType(Map.class)
            }
            def page = 1
            def maxResult = 20;
            def extParams = [:]
            boolean countFlag = true;
            if(!this.dbVersion.startsWith("2")){
                countFlag = false
            }

            if(args.size()>1 && args[1] instanceof Map ){
                extParams = args[1]

                if(args[2]){
                    page = args[2].toInteger()
                }
                if(args[3]){
                    maxResult = args[3].toInteger()
                }
                if (args[4] && (args[4] instanceof Boolean)) {
                    countFlag = args[4].toBoolean()
                }

            }else {
                if(args[1]){
                    page = args[1].toInteger()
                }
                if(args[2]){
                    maxResult = args[2].toInteger()
                }
                if (args[3] && (args[3] instanceof Boolean)) {
                    countFlag = args[3].toBoolean()
                }
            }
            def ret = innerSearchRecord(coll,query, page, maxResult, countFlag, normalCallBack, extParams)
            return ret
        } else if (name.startsWith("del")) {
            def q = [:]
            if(args[0] instanceof Map){
                q = args[0]
            }
            def query = mapToBson(q)
            try {
                innerGetColl(name, 3).deleteMany(query)

            } catch (e) {
                e.printStackTrace();
                throw e;
            }
        } else if (name.startsWith("callBackInFind")) {
            args = args as List
            def coll = innerGetColl(name, 14)
            def callbackWrap = { item, list ->
                args[1](item)
            }
            def ret = innerSearchRecord(coll, args[0]?.asType(Map.class), 1, 100000000, false, callbackWrap)

            return ret
        } else if (name.startsWith("dropTable")) {
//            args = args as List
            def coll = innerGetColl(name, 9)
            def ret = coll.drop()

            return ret
        } else if (name.startsWith("aggregateWithPipe")) { //使用对象来聚合
            def coll = innerGetColl(name, 17)
            def pipelineList = []
            args[0].each {
                pipelineList << mapToBson(it)
            }
            def ret = []
            coll.aggregate(pipelineList).into(ret);
            if (ret?.size() > 0) {
                return ret
            } else {
                return [:]
            }
        } else if (name.startsWith("aggregate")) { //使用字符串来聚合
            println name
            def coll = innerGetColl(name, 9)
            def pipelineList = []
            args[0].split("@@@").each {
                pipelineList << mapToBson(JSON.parse(it))
            }
            def ret = []
            coll.aggregate(pipelineList).into(ret);

            if (ret?.size() > 0) {
                return ret;
            }
            return null

        }else if(name.startsWith("createIndex")){
            def coll = innerGetColl(name, 11)
            def q = [:]
            if(args[0] instanceof Map){
                q = args[0]
            }
            if(q){
                def index_info = mapToBson(q)
                IndexOptions opt = new IndexOptions()
                opt.background(true);
                if(args.size()>1 && args[1] && (args[1] instanceof Map)){
                    def options = args[1]
                    if(options.expire && (options.expire instanceof Integer)){
                        opt.expireAfter(options.expire, TimeUnit.SECONDS)
                    }
                    if(options.unique && (options.expire instanceof Boolean)){
                        opt.unique(options.unique)
                    }
                    if(options.background && (options.background instanceof Boolean)){
                        opt.background(options.background)
                    }
                }
                return coll.createIndex(index_info,opt)
            }else {
                return null
            }
        }else if(name.startsWith("dropIndex")){
            def coll = innerGetColl(name, 9)
            if(args[0] instanceof Map){
                coll.dropIndex(mapToBson(args[0]))
                return true
            }else if(args[0] instanceof String){
                coll.dropIndex(args[0].toString().trim())
                return true
            }
            return false
        }

        return "unknown method $name(${args.join(',')})"
    }

    /**********查询操作***********/
    /*
     * query  查询条件, 为一个BasicDBObject
     * getCount指是否需要查询总数，默认为需要
     * 返回PageBean结构的map
     * callBack(cur.next())
     */
    def innerSearchRecord = { coll, query = [:], page = 1, maxResult = 20, getCount = false, callBack = null, extParams = null ->
        if (!page) {//为什么要这么写一次，是因为传进来的参数可能是null,这样默认值就不起作用了
            page = 1
        }
        if (maxResult == null) {
            maxResult = 20
        }
        if (getCount instanceof Boolean) {
            getCount = getCount
        }else {
            getCount = false;
        }
        def sortMap
        if (query.sort) {
            sortMap = query.sort
            query.remove("sort")
        }
        if (query.limit) {
            maxResult = query.limit.toInteger()
            query.remove("limit")
        }
        def bean = [contentlist: [], maxResult: maxResult]
        query = mapToBson(query)
        def resultList = []
        def cur
        try {
//			cur= coll.find(query,mapToBson(fileds))
            cur = coll.find(query)
            def fileds = new Document();
            if(extParams){
                if (extParams?.exclude) {
                    extParams.exclude.each { fileds."${it}" = 0 }
                } else if (extParams?.include) {
                    extParams.include.each { fileds."${it}" = 1 }
                }
            }

            if (fileds) {
                cur = cur.projection(fileds);
            }
            if (sortMap) {
                sortMap = mapToBson(sortMap)
                cur = cur.sort(sortMap)
            }
            cur = cur.skip((page - 1) * maxResult).limit(maxResult).iterator();
            while (cur.hasNext()) {
                if (callBack) {
                    callBack(cur.next(), bean.contentlist)
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//			cur.close()
        }

        int allNum = 1000
        if (getCount) {
//            allNum = coll.countDocuments(query) //java-mongo-driver 3.12以上驱动版本用
            allNum = coll.count(query)
        }
        int allPage = 0
        int temp = allNum;
        if (allNum % maxResult == 0)
            allPage = allNum / maxResult;
        else
            allPage = allNum / maxResult + 1;

        bean.allNum = allNum
        bean.allPages = allPage
        bean.currentPage = page
        return bean
    }

    //从map转为BSON
    //在saveAll操作时,要手动添加_id=new ObjectId() 和ct
    //save操作时,会自动添加_id和ct
    //在操作数据库时,存入的数据必须是基础类型\map\list三者之一,不能存其他对象
    def mapToBson = { obj ->
        if (obj instanceof Map) {
            def condition = new Document()
            obj.keySet().each {
                condition."${it}" = mapToBson(obj."${it}")
            }
            return condition
        } else if (obj instanceof Collection) {
            def list = []
            obj.each {
                def condition = mapToBson(it)
                list << condition
            }
            return list
        } else if (obj instanceof GString){
            return obj.toString()
        } else {
            return obj
        }
    }


    //把map中非set的值提出来
    def innerTranslateMap = { modify,noSet ->
        def map = [:]
        Iterator itor = modify.keySet().iterator()
        while (itor.hasNext()){
            def key = itor.next().toString()
            if(key.startsWith("\$")){
                noSet."${key}" = modify."${key}"
            }else{
                map."${key}" = modify."${key}"
            }
        }
//        modify.clear()
        return map
    }

    //根据方法全名取得coll
    def innerGetColl = { name, num ->
        def collName = name.substring(num)
        def first = collName.substring(0, 1).toLowerCase()
        collName = first + collName.substring(1) //取得collName
//        MongoCollection coll = this.db.getCollection(collName);
        MongoCollection coll = this.db.getCollection(collName);
//		coll.countDocuments()
        return coll
    }

    //一个最为简单的操作处理器
    def normalCallBack = { item, list ->
        list << item //简单地加入记录
    }

//
//    //执行事务
//    def doTxn={callback ->
//        boolean flag = false;
//		if(this.dbVersion&&this.dbVersion[0].toString().toInteger()<4) {
//			println "-----mongodb版本低于4.0,不支持事务操作."
//			return flag;
//		}
//        final ClientSession session = this.mongoClient.startSession()
//        TransactionOptions txnOptions = TransactionOptions.builder()
//                .readPreference(ReadPreference.primary())
//                .readConcern(ReadConcern.LOCAL)
//                .writeConcern(WriteConcern.MAJORITY)
//                .build();
//
//        TransactionBody txnBody = new TransactionBody<Object>() {
//            public Object execute() {
//                return callback()
//            }
//        };
//        try {
//            session.withTransaction(txnBody, txnOptions);
//            flag = true
//        } catch (RuntimeException e) {
//            // some error handling
//            e.printStackTrace()
//            flag = false;
//        } finally {
//            session.close();
//        }
//
//        return flag
//    }


}