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
#include <DataStreams/OneBlockInputStream.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>

#include <Common/getFQDNOrHostName.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>

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
    extern const int TIMEOUT_EXCEEDED;
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
    queue_dir = zk_root_dir;
    if (queue_dir.back() == '/')
        queue_dir.resize(queue_dir.size() - 1);

    host_name = getFQDNOrHostName();
    port = context.getTCPPort();
    host_id = host_name + ':' + DB::toString(port);

    event_queue_updated = std::make_shared<Poco::Event>();

    thread = std::thread(&DDLWorker::run, this);
}


DDLWorker::~DDLWorker()
{
    stop_flag = true;
    event_queue_updated->set();
    thread.join();
}


void DDLWorker::processTasks()
{
    LOG_DEBUG(log, "Processing tasks");

    Strings queue_nodes = zookeeper->getChildren(queue_dir, nullptr, event_queue_updated);
    if (queue_nodes.empty())
        return;

    bool server_startup = last_processed_node_name.empty();

    std::sort(queue_nodes.begin(), queue_nodes.end());
    auto begin_node = server_startup
        ? queue_nodes.begin()
        : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_processed_node_name);

    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String node_data, node_name = *it, node_path = queue_dir + "/" + node_name;

        if (!zookeeper->tryGet(node_path, node_data))
        {
            /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
            continue;
        }

        DDLLogEntry node;
        node.parse(node_data);

        bool host_in_hostlist = std::find(node.hosts.cbegin(), node.hosts.cend(), host_id) != node.hosts.cend();
        bool already_processed = zookeeper->exists(node_path + "/finished/" + host_id);

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
                tryLogCurrentException(log, "An error occurred while processing node " + node_name + " (" + node.query + ")");
                throw;
            }
        }
        else
        {
            LOG_DEBUG(log, "Node " << node_name << " (" << node.query << ") will not be processed");
        }

        last_processed_node_name = node_name;
    }
}


static bool tryExecuteQuery(const String & query, Context & context, ExecutionStatus & status, Logger * log = nullptr)
{
    try
    {
        executeQuery(query, context);
    }
    catch (...)
    {
        status = ExecutionStatus::fromCurrentException();

        if (log)
            tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");

        return false;
    }

    status = ExecutionStatus(0);
    if (log)
        LOG_DEBUG(log, "Executed query: " << query);

    return true;
}


void DDLWorker::processTask(const DDLLogEntry & node, const std::string & node_name)
{
    LOG_DEBUG(log, "Processing node " << node_name << " (" << node.query << ")");

    String node_path = queue_dir + "/" + node_name;
    createStatusDirs(node_path);

    bool should_not_execute = current_node == node_name && current_node_was_executed;

    if (!should_not_execute)
    {
        current_node = node_name;
        current_node_was_executed = false;

        zookeeper->create(node_path + "/active/" + host_id, "", zkutil::CreateMode::Ephemeral);

        try
        {
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

            const auto & host_address = cluster->getShardsWithFailoverAddresses().at(shard_num).at(host_num);
            ASTPtr rewritten_ast = query->getRewrittenASTWithoutOnCluster(host_address.default_database);
            String rewritten_query = queryToString(rewritten_ast);

            LOG_DEBUG(log, "Executing query: " << rewritten_query);

            if (auto query_alter = dynamic_cast<const ASTAlterQuery *>(rewritten_ast.get()))
            {
                processTaskAlter(query_alter, rewritten_query, cluster, shard_num, node_path);
            }
            else
            {
                tryExecuteQuery(rewritten_query, context, current_node_execution_status, log);
            }
        }
        catch (const zkutil::KeeperException & e)
        {
            throw;
        }
        catch (...)
        {
            current_node_execution_status = ExecutionStatus::fromCurrentException("An error occured during query preparation");
        }

        /// We need to distinguish ZK errors occured before and after query executing
        current_node_was_executed = true;
    }

    /// Delete active flag and create finish flag
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(node_path + "/active/" + host_id, -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/finished/" + host_id,
        current_node_execution_status.serializeText(), zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));

    int code = zookeeper->tryMultiWithRetries(ops);
    if (code != ZOK && code != ZNONODE)
        throw zkutil::KeeperException("Cannot commit executed node " + node_name, code);
}


void DDLWorker::processTaskAlter(
    const ASTAlterQuery * query_alter,
    const String & rewritten_query,
    const std::shared_ptr<Cluster> & cluster,
    ssize_t shard_num,
    const String & node_path)
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

    const auto & shard_info = cluster->getShardsInfo().at(shard_num);
    bool config_is_replicated_shard = shard_info.hasInternalReplication();

    if (execute_once_on_replica && !config_is_replicated_shard)
    {
        throw Exception("Table " + query_alter->table + " is replicated, but shard #" + toString(shard_num + 1) +
            " isn't replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }
    else if (!execute_once_on_replica && config_is_replicated_shard)
    {
        throw Exception("Table " + query_alter->table + " isn't replicated, but shard #" + toString(shard_num + 1) +
            " replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }

    if (execute_once_on_replica)
    {
        Strings replica_names;
        for (const auto & address : cluster->getShardsWithFailoverAddresses().at(shard_num))
            replica_names.emplace_back(address.toString());
        std::sort(replica_names.begin(), replica_names.end());

        String shard_dir_name;
        for (auto it = replica_names.begin(); it != replica_names.end(); ++it)
            shard_dir_name += *it + (std::next(it) != replica_names.end() ? "," : "");

        String shard_path = node_path + "/shards/" + shard_dir_name;
        String is_executed_path = shard_path + "/executed";
        zookeeper->createAncestors(shard_path + "/");

        bool alter_executed_by_replica = false;
        {
            auto zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
            zookeeper_holder->initFromInstance(zookeeper);

            zkutil::Lock lock(zookeeper_holder, shard_path, "lock", host_id);
            std::mt19937 rng(std::hash<String>{}(host_id) + reinterpret_cast<intptr_t>(&rng));

            for (int num_tries = 0; num_tries < 10; ++num_tries)
            {
                if (zookeeper->exists(is_executed_path))
                {
                    alter_executed_by_replica = true;
                    break;
                }

                if (lock.tryLock())
                {
                    tryExecuteQuery(rewritten_query, context, current_node_execution_status, log);

                    if (execute_on_leader_replica && current_node_execution_status.code == ErrorCodes::NOT_IMPLEMENTED)
                    {
                        /// TODO: it is ok to recieve exception "host is not leader"
                    }

                    zookeeper->create(is_executed_path, host_id, zkutil::CreateMode::Persistent);
                    lock.unlock();
                    alter_executed_by_replica = true;
                    break;
                }

                std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, 1)(rng)));
            }
        }

        if (!alter_executed_by_replica)
            current_node_execution_status = ExecutionStatus(ErrorCodes::NOT_IMPLEMENTED, "Cannot enqueue replicated DDL query");
    }
    else
    {
        tryExecuteQuery(rewritten_query, context, current_node_execution_status, log);
    }
}


void DDLWorker::cleanupQueue(const Strings * node_names_to_check)
{
    /// Both ZK and Poco use Unix epoch
    size_t current_time_seconds = Poco::Timestamp().epochTime();
    constexpr size_t zookeeper_time_resolution = 1000;

    // Too early to check
    if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_min_period_seconds)
        return;

    last_cleanup_time_seconds = current_time_seconds;

    LOG_DEBUG(log, "Cleaning queue");

    String data;
    zkutil::Stat stat;
    DDLLogEntry node;

    Strings node_names_fetched = node_names_to_check ? Strings{} : zookeeper->getChildren(queue_dir);
    const Strings & node_names = (node_names_to_check) ? *node_names_to_check : node_names_fetched;

    for (const String & node_name : node_names)
    {
        try
        {
            String node_path = queue_dir + "/" + node_name;
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

            Strings finished_nodes = zookeeper->getChildren(node_path + "/finished");
            node.parse(data);

            if (finished_nodes.size() >= node.hosts.size())
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
    zkutil::Ops ops;
    auto acl = zookeeper->getDefaultACL();
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/active", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/finished", "", acl, zkutil::CreateMode::Persistent));

    int code = zookeeper->tryMulti(ops);
    if (code != ZOK && code != ZNODEEXISTS)
        throw zkutil::KeeperException(code);
}


String DDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    if (entry.hosts.empty())
        return {};

    String query_path_prefix = queue_dir + "/query-";
    zookeeper->createAncestors(query_path_prefix);

    String node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);
    createStatusDirs(node_path);

    return node_path;
}


void DDLWorker::run()
{
    setThreadName("DDLWorker");
    LOG_DEBUG(log, "Started DDLWorker thread");

    zookeeper = context.getZooKeeper();
    zookeeper->createAncestors(queue_dir + "/");

    while (!stop_flag)
    {
        try
        {
            processTasks();

            LOG_DEBUG(log, "Waiting watch");
            event_queue_updated->wait();

            if (stop_flag)
                break;

            cleanupQueue();
        }
        catch (zkutil::KeeperException &)
        {
            LOG_DEBUG(log, "Recovering ZooKeeper session");
            zookeeper = context.getZooKeeper();
        }
        catch (...)
        {
            tryLogCurrentException(log);
            throw;
        }
    }
}


class DDLQueryStatusInputSream : public IProfilingBlockInputStream
{
public:

    DDLQueryStatusInputSream(const String & zk_node_path, Context & context, size_t num_hosts)
    : node_path(zk_node_path), context(context), watch(CLOCK_MONOTONIC_COARSE)
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

    static constexpr size_t timeout_seconds = 120;

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

            auto elapsed_seconds = watch.elapsedSeconds();
            if (elapsed_seconds > timeout_seconds)
                throw Exception("Watching query is executing too long (" + toString(elapsed_seconds) + " sec.)", ErrorCodes::TIMEOUT_EXCEEDED);

            if (num_hosts_finished != 0 || try_number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(20LU, try_number + 1)));

            /// TODO: add shared lock
            if (!zookeeper->exists(node_path))
            {
                throw Exception("Cannot provide query execution status. The query's node " + node_path
                                + " had been deleted by cleaner since it was finished (or its lifetime is expired)",
                                ErrorCodes::UNFINISHED);
            }

            Strings new_hosts = getNewAndUpdate(finished_hosts_set, getChildrenAllowNoNode(zookeeper, node_path + "/finished"));
            ++try_number;
            if (new_hosts.empty())
                continue;

            Strings cur_active_hosts = getChildrenAllowNoNode(zookeeper, node_path + "/active");

            res = sample.cloneEmpty();
            for (const String & host : new_hosts)
            {
                ExecutionStatus status(1, "Cannot obtain error message");
                {
                    String status_data;
                    if (zookeeper->tryGet(node_path + "/finished/" + host, status_data))
                        status.deserializeText(status_data);
                }

                res.getByName("host").column->insert(host);
                res.getByName("status").column->insert(static_cast<UInt64>(status.code));
                res.getByName("error").column->insert(status.message);
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

    Stopwatch watch;

    NameSet finished_hosts_set;
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
