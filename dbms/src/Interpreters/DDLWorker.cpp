#include <Interpreters/DDLWorker.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>

#include <Storages/IStorage.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Common/getFQDNOrHostName.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>

#include <zkutil/ZooKeeper.h>
#include <zkutil/Lock.h>
#include <Poco/Timestamp.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int INCONSISTENT_TABLE_ACCROSS_SHARDS;
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
    extern const int UNFINISHED;
}


struct DDLLogEntry
{
    String query;
    Strings hosts;
    String initiator;

    static constexpr char CURRENT_VERSION = '1';

    String toString()
    {
        String res;
        {
            WriteBufferFromString wb(res);

            writeChar(CURRENT_VERSION, wb);
            wb << "\n";
            wb << "query: " << query << "\n";
            wb << "hosts: " << hosts << "\n";
            wb << "initiator: " << initiator << "\n";
        }

        return res;
    }

    void parse(const String & data)
    {
        ReadBufferFromString rb(data);

        char version;
        readChar(version, rb);
        if (version != CURRENT_VERSION)
            throw Exception("Unknown DDLLogEntry format version: " + version, ErrorCodes::UNKNOWN_FORMAT_VERSION);

        rb >> "\n";
        rb >> "query: " >> query >> "\n";
        rb >> "hosts: " >> hosts >> "\n";
        rb >> "initiator: " >> initiator >> "\n";

        assertEOF(rb);
    }
};


// static String serializeReturnStatus(int return_code, const String & return_msg)
// {
//     String res;
//     {
//         WriteBufferFromString wb(res);
//         wb << return_code << "\n" << return_msg;
//     }
//     return res;
// }
//
//
// static void parseReturnStatus(const String & data, int & return_code, String & return_msg)
// {
//     ReadBufferFromString rb(data);
//     rb >> return_code >> "\n" >> return_msg;
// }


static const std::pair<ssize_t, ssize_t> tryGetShardAndHostNum(const Cluster::AddressesWithFailover & cluster, const String & host_name, UInt16 port)
{
    for (size_t shard_num = 0; shard_num < cluster.size(); ++shard_num)
    {
        for (size_t host_num = 0; host_num < cluster[shard_num].size(); ++host_num)
        {
            const Cluster::Address & address = cluster[shard_num][host_num];
            if (address.host_name == host_name && address.port == port)
                return {shard_num, host_num};
        }
    }

    return {-1, -1};
}

// static const Cluster::Address * tryGetAddressOfHost(const Cluster::AddressesWithFailover & cluster, const String & host_name, UInt16 port)
// {
//     std::pair<ssize_t, ssize_t> host_pos = tryGetShardAndHostNum(cluster, host_name, port);
//
//     if (host_pos.first < 0 || host_pos.second < 0)
//         return nullptr;
//
//     return &cluster.at(host_pos.first).at(host_pos.second);
// }

static bool isSupportedAlterType(int type)
{
    static const std::unordered_set<int> supported_alter_types{
        ASTAlterQuery::ADD_COLUMN,
        ASTAlterQuery::DROP_COLUMN,
        ASTAlterQuery::MODIFY_COLUMN,
        ASTAlterQuery::MODIFY_PRIMARY_KEY,
        ASTAlterQuery::DROP_PARTITION
    };

    return supported_alter_types.count(type);
}


DDLWorker::DDLWorker(const std::string & zk_root_dir, Context & context_)
    : context(context_), stop_flag(false)
{
    root_dir = zk_root_dir;
    if (root_dir.back() == '/')
        root_dir.resize(root_dir.size() - 1);

    host_name = getFQDNOrHostName();
    port = context.getTCPPort();
    host_id = host_name + ':' + DB::toString(port);

    event_queue_updated = std::make_shared<Poco::Event>();

    thread = std::thread(&DDLWorker::run, this);
}


DDLWorker::~DDLWorker()
{
    stop_flag = true;
    //cond_var.notify_one();
    event_queue_updated->set();
    thread.join();
}


void DDLWorker::processTasks()
{
    auto zookeeper = context.getZooKeeper();
    LOG_DEBUG(log, "processTasks");

    Strings queue_nodes = zookeeper->getChildren(root_dir, nullptr, event_queue_updated);
    if (queue_nodes.empty())
        return;

    bool server_startup = last_processed_node_name.empty();

    std::sort(queue_nodes.begin(), queue_nodes.end());
    auto begin_node = server_startup
        ? queue_nodes.begin()
        : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_processed_node_name);

    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String node_data, node_name = *it, node_path = root_dir + "/" + node_name;
        LOG_DEBUG(log, "Fetching node " << node_path);
        if (!zookeeper->tryGet(node_path, node_data))
        {
            /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
            continue;
        }

        DDLLogEntry node;
        node.parse(node_data);

        bool host_in_hostlist = std::find(node.hosts.cbegin(), node.hosts.cend(), host_id) != node.hosts.cend();

        bool already_processed = zookeeper->exists(node_path + "/failed/" + host_id)
                                 || zookeeper->exists(node_path + "/sucess/" + host_id);

        LOG_DEBUG(log, "Checking node " << node_name << ", " << node.query << " status: " << host_in_hostlist << " " << already_processed);

        if (!server_startup && already_processed)
        {
            throw Exception(
                "Server expects that DDL node " + node_name + " should be processed, but it was already processed according to ZK",
                ErrorCodes::LOGICAL_ERROR);
        }

        if (host_in_hostlist && !already_processed)
        {
            try
            {
                processTask(node, node_name);
            }
            catch (...)
            {
                /// It could be network error, but we mark node as processed anyway.
                last_processed_node_name = node_name;

                tryLogCurrentException(log,
                    "An unexpected error occurred during processing DLL query " + node.query + " (" + node_name + ")");
                throw;
            }
        }

        last_processed_node_name = node_name;
    }
}


static bool tryExecuteQuery(const String & query, Context & context, int & return_code, String & exception, Logger * log)
{
    try
    {
        executeQuery(query, context);
    }
    catch (...)
    {
        exception = getCurrentExceptionMessage(false, true);
        return_code = getCurrentExceptionCode();
        if (log)
            tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");

        return false;
    }

    return_code = 0;
    exception = "";
    if (log)
        LOG_DEBUG(log, "Executed query: " << query);

    return true;
}


bool DDLWorker::processTask(const DDLLogEntry & node, const std::string & node_name)
{
    auto zookeeper = context.getZooKeeper();
    LOG_DEBUG(log, "Process " << node_name << " node, query " << node.query);

    String node_path = root_dir + "/" + node_name;
    createStatusDirs(node_path);

    String active_flag_path = node_path + "/active/" + host_id;
    zookeeper->create(active_flag_path, "", zkutil::CreateMode::Ephemeral);


    LOG_DEBUG(log, "Process query: " << node.query);


    ASTPtr query_ast;
    {
        ParserQuery parser_query;
        String description;
        IParser::Pos begin = &node.query.front();
        query_ast = parseQuery(parser_query, begin, begin + node.query.size(), description);
    }

    const ASTQueryWithOnCluster * query;
    if (!query_ast || !(query = dynamic_cast<const ASTQueryWithOnCluster *>(query_ast.get())))
        throw Exception("Recieved unsupported DDL query", ErrorCodes::NOT_IMPLEMENTED);

    String cluster_name = query->cluster;
    auto cluster = context.getCluster(cluster_name);

    ssize_t shard_num, host_num;
    std::tie(shard_num, host_num) = tryGetShardAndHostNum(cluster->getShardsWithFailoverAddresses(), host_name, port);
    if (shard_num < 0 || host_num < 0)
    {
        throw Exception("Cannot find own address (" + host_id + ") in cluster " + cluster_name + " configuration",
                        ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }

    const auto & shard = cluster->getShardsWithFailoverAddresses().at(shard_num);
    const auto & host_address = shard.at(host_num);
    ASTPtr rewritten_ast = query->getRewrittenASTWithoutOnCluster(host_address.default_database);
    String rewritten_query = queryToString(rewritten_ast);


    LOG_DEBUG(log, "Executing query: " << rewritten_query);

    int result_code = 0;
    String result_message;


    if (auto query_alter = dynamic_cast<const ASTAlterQuery *>(rewritten_ast.get()))
    {
        String database = query_alter->database.empty() ? context.getCurrentDatabase() : query_alter->database;
        StoragePtr storage = context.getTable(database, query_alter->table);

        bool execute_once_on_replica = storage->supportsReplication();
        bool execute_on_leader_replica = false;

        for (const auto & param : query_alter->parameters)
        {
            if (!isSupportedAlterType(param.type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);

            if (execute_once_on_replica)
                execute_on_leader_replica |= param.type == ASTAlterQuery::DROP_PARTITION;
        }

        if (execute_once_on_replica)
        {
            const auto & shard_info = cluster->getShardsInfo().at(shard_num);
            if (!shard_info.hasInternalReplication())
            {
                throw Exception("Table " + query_alter->table + " is replicated, but shard #" + toString(shard_num + 1) +
                    " isn't replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
            }

            Strings replica_names;
            for (const auto & address : shard)
                replica_names.emplace_back(address.toString());
            std::sort(replica_names.begin(), replica_names.end());

            String shard_dir_name;
            for (auto it = replica_names.begin(); it != replica_names.end(); ++it)
                shard_dir_name += *it + (std::next(it) != replica_names.end() ? "," : "");

            String shard_path = node_path + "/shards/" + shard_dir_name;
            String is_executed_path = shard_path + "/executed";
            zookeeper->createAncestors(shard_path + "/");

            bool executed = false;
            {
                auto zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
                zookeeper_holder->initFromInstance(zookeeper);

                zkutil::Lock lock(zookeeper_holder, shard_path, "lock", host_id);
                std::mt19937 rng(std::hash<String>{}(host_id) + reinterpret_cast<intptr_t>(&rng));
                for (int num_tries = 0; num_tries < 10; ++num_tries)
                {
                    if (zookeeper->exists(is_executed_path))
                    {
                        executed = true;
                        break;
                    }

                    if (lock.tryLock())
                    {
                        tryExecuteQuery(rewritten_query, context, result_code, result_message, log);

                        if (execute_on_leader_replica && result_code == ErrorCodes::NOT_IMPLEMENTED)
                        {
                            /// TODO: it is ok to recieve exception "host is not leader"
                        }

                        zookeeper->create(is_executed_path, host_id, zkutil::CreateMode::Persistent);
                        lock.unlock();
                        executed = true;
                        break;
                    }

                    std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, 1)(rng)));
                }
            }

            if (!executed)
            {
                result_code = ErrorCodes::NOT_IMPLEMENTED;
                result_message = "Cannot enqueue replicated DDL query";
            }
        }
        else
        {
            tryExecuteQuery(rewritten_query, context, result_code, result_message, log);
        }
    }
    else
    {
        tryExecuteQuery(rewritten_query, context, result_code, result_message, log);
    }

    /// Delete active flag and create sucess (or fail) flag
    zkutil::Ops ops;
    String result_path = node_path + (result_code ? "/failed/" : "/sucess/") + host_id;
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(active_flag_path, -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(result_path, result_message, zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);

    return !result_code;
}


void DDLWorker::cleanupQueue(const Strings * node_names_to_check)
{
    auto zookeeper = context.getZooKeeper();

    /// Both ZK and Poco use Unix epoch
    size_t current_time_seconds = Poco::Timestamp().epochTime();
    constexpr size_t zookeeper_time_resolution = 1000;

    // Too early to check
    if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_after_seconds)
        return;

    last_cleanup_time_seconds = current_time_seconds;

    String data;
    zkutil::Stat stat;
    DDLLogEntry node;
    Strings failed_hosts, sucess_hosts;

    Strings node_names_fetched = node_names_to_check ? Strings{} : zookeeper->getChildren(root_dir);
    const Strings & node_names = (node_names_to_check) ? *node_names_to_check : node_names_fetched;

    for (const String & node_name : node_names)
    {
        try
        {
            String node_path = root_dir + "/" + node_name;
            if (!zookeeper->tryGet(node_path, data, &stat))
                continue;

            /// TODO: Add shared lock to avoid rare race counditions.

            size_t zookeeper_time_seconds = stat.mtime / zookeeper_time_resolution;
            if (zookeeper_time_seconds + node_max_lifetime_seconds < current_time_seconds)
            {
                size_t lifetime_seconds = current_time_seconds - zookeeper_time_seconds;
                LOG_INFO(log, "Lifetime of node " << node_name << " (" << lifetime_seconds << " sec.) is expired, deleting it");
                zookeeper->removeRecursive(node_path);
                continue;
            }

            Strings sucess_nodes = zookeeper->getChildren(node_path + "/sucess");
            Strings failed_nodes = zookeeper->getChildren(node_path + "/failed");

            node.parse(data);

            if (sucess_nodes.size() + failed_nodes.size() >= node.hosts.size())
            {
                LOG_INFO(log, "Node " << node_name << " had been executed by each host, deleting it");
                zookeeper->removeRecursive(node_path);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occured while checking and cleaning node " + node_name + " from queue");
        }
    }
}


/// Try to create unexisting "status" dirs for a node
void DDLWorker::createStatusDirs(const std::string & node_path)
{
    auto zookeeper = context.getZooKeeper();
    auto acl = zookeeper->getDefaultACL();

    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/active", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/sucess", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/failed", "", acl, zkutil::CreateMode::Persistent));

    int code = zookeeper->tryMulti(ops);

    if (code != ZOK && code != ZNODEEXISTS)
        throw zkutil::KeeperException(code);
}


String DDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    if (entry.hosts.empty())
        return {};

    auto zookeeper = context.getZooKeeper();

    String query_path_prefix = root_dir + "/query-";
    zookeeper->createAncestors(query_path_prefix);

    String node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);
    createStatusDirs(node_path);

    return node_path;
}


void DDLWorker::run()
{
    using namespace std::chrono_literals;
    auto zookeeper = context.getZooKeeper();

    zookeeper->createAncestors(root_dir + "/");
    LOG_DEBUG(log, "Started DDLWorker thread");

    while (!stop_flag)
    {
        LOG_DEBUG(log, "Begin tasks processing");

        try
        {
            processTasks();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        //std::unique_lock<std::mutex> g(lock);
        //cond_var.wait_for(g, 10s);
        LOG_DEBUG(log, "Waiting watch");
        event_queue_updated->wait();

        try
        {
            cleanupQueue();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}


class DDLQueryStatusInputSream : public IProfilingBlockInputStream
{
public:

    DDLQueryStatusInputSream(const String & zk_node_path, Context & context, size_t num_hosts)
    : node_path(zk_node_path), context(context)
    {
        sample = Block{
            {std::make_shared<DataTypeString>(),    "host"},
            {std::make_shared<DataTypeUInt64>(),    "status"},
            {std::make_shared<DataTypeString>(),    "error"},
            {std::make_shared<DataTypeUInt64>(),    "num_hosts_remaining"},
            {std::make_shared<DataTypeUInt64>(),    "num_hosts_active"},
        };

        setTotalRowsApprox(num_hosts);
    }

    String getName() const override
    {
        return "DDLQueryStatusInputSream";
    }

    String getID() const override
    {
        return "DDLQueryStatusInputSream(" + node_path + ")";
    }

    Block readImpl() override
    {
        Block res;
        if (num_hosts_finished >= total_rows_approx)
            return res;

        auto zookeeper = context.getZooKeeper();
        size_t try_number = 0;

        while(res.rows() == 0)
        {
            if (is_cancelled)
                return res;

            if (num_hosts_finished != 0 || try_number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(20LU, try_number + 1)));

            /// TODO: add /lock node somewhere
            if (!zookeeper->exists(node_path))
            {
                throw Exception("Cannot provide query execution status. The query's node " + node_path
                                + " had been deleted by cleaner since it was finished (or its lifetime is expired)",
                                ErrorCodes::UNFINISHED);
            }

            Strings new_sucess_hosts = getNewAndUpdate(sucess_hosts_set, getChildrenAllowNoNode(zookeeper, node_path + "/sucess"));
            Strings new_failed_hosts = getNewAndUpdate(failed_hosts_set, getChildrenAllowNoNode(zookeeper, node_path + "/failed"));

            Strings new_hosts = new_sucess_hosts;
            new_hosts.insert(new_hosts.end(), new_failed_hosts.cbegin(), new_failed_hosts.cend());
            ++try_number;

            if (new_hosts.empty())
                continue;

            Strings cur_active_hosts = getChildrenAllowNoNode(zookeeper, node_path + "/active");

            res = sample.cloneEmpty();
            for (size_t i = 0; i < new_hosts.size(); ++i)
            {
                bool fail = i >= new_sucess_hosts.size();
                res.getByName("host").column->insert(new_hosts[i]);
                res.getByName("status").column->insert(static_cast<UInt64>(fail));
                res.getByName("error").column->insert(fail ? zookeeper->get(node_path + "/failed/" + new_hosts[i]) : String{});
                res.getByName("num_hosts_remaining").column->insert(total_rows_approx - (++num_hosts_finished));
                res.getByName("num_hosts_active").column->insert(cur_active_hosts.size());
            }
        }

        return res;
    }

    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
    {
        Strings res;
        int code = zookeeper->tryGetChildren(node_path, res);
        if (code != ZOK && code != ZNONODE)
            throw zkutil::KeeperException(code, node_path);
        return res;
    }

    static Strings getNewAndUpdate(NameSet & prev, const Strings & cur_list)
    {
        Strings diff;
        for (const String & elem : cur_list)
        {
            if (!prev.count(elem))
            {
                diff.emplace_back(elem);
                prev.emplace(elem);
            }
        }

        return diff;
    }

    ~DDLQueryStatusInputSream() override = default;

    Block sample;

private:
    String node_path;
    Context & context;

    NameSet sucess_hosts_set;
    NameSet failed_hosts_set;
    size_t num_hosts_finished = 0;
};


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, Context & context)
{
    const auto query = dynamic_cast<const ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Distributed execution is not supported for such DDL queries",
                        ErrorCodes::NOT_IMPLEMENTED);
    }

    auto query_alter = dynamic_cast<const ASTAlterQuery *>(query_ptr.get());
    if (query_alter)
    {
        for (const auto & param : query_alter->parameters)
        {
            if (!isSupportedAlterType(param.type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    ClusterPtr cluster = context.getCluster(query->cluster);
    DDLWorker & ddl_worker = context.getDDLWorker();

    DDLLogEntry entry;
    entry.query = queryToString(query_ptr);
    entry.initiator = ddl_worker.getHostName();

    Cluster::AddressesWithFailover shards = cluster->getShardsWithFailoverAddresses();
    for (const auto & shard : shards)
    {
        for (const auto & addr : shard)
            entry.hosts.emplace_back(addr.toString());
    }

    String node_path = ddl_worker.enqueueQuery(entry);

    BlockIO io;
    if (node_path.empty())
        return io;

    auto stream = std::make_shared<DDLQueryStatusInputSream>(node_path, context, entry.hosts.size());
    io.in_sample = stream->sample.cloneEmpty();
    io.in = std::move(stream);
    return io;
}


}
