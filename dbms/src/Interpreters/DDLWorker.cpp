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

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Lock.h>
#include <Common/isLocalAddress.h>
#include <Poco/Timestamp.h>

#include <experimental/optional>


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
    extern const int UNKNOWN_TYPE_OF_QUERY;
}


const size_t DDLWorker::node_max_lifetime_seconds = 7 * 24 * 60 * 60; // week
const size_t DDLWorker::cleanup_min_period_seconds = 60; // minute

struct DDLLogEntry
{
    String query;
    Strings hosts;
    String initiator; // optional

    static constexpr int CURRENT_VERSION = 1;

    String toString()
    {
        WriteBufferFromOwnString wb;

        auto version = CURRENT_VERSION;
        wb << "version: " << version << "\n";
        wb << "query: " << escape << query << "\n";
        wb << "hosts: " << hosts << "\n";
        wb << "initiator: " << initiator << "\n";

        return wb.str();
    }

    void parse(const String & data)
    {
        ReadBufferFromString rb(data);

        int version;
        rb >> "version: " >> version >> "\n";

        if (version != CURRENT_VERSION)
            throw Exception("Unknown DDLLogEntry format version: " + DB::toString(version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

        rb >> "query: " >> escape >> query >> "\n";
        rb >> "hosts: " >> hosts >> "\n";

        if (!rb.eof())
            rb >> "initiator: " >> initiator >> "\n";
        else
            initiator.clear();

        assertEOF(rb);
    }
};


struct DDLTask
{
    DDLLogEntry entry;
    String entry_name;

    ASTPtr query;
    ASTQueryWithOnCluster * query_on_cluster = nullptr;

    String cluster_name;
    ClusterPtr cluster;

    Cluster::Address host_address;
    String host_id_in_cluster;
    size_t host_shard_num;
    size_t host_replica_num;

    /// Parses entry and query, extracts cluster and finds current host in the cluster
    /// Return true if current host is found in the cluster
    bool fillFromEntryData(const String & entry_data, const String & entry_name_, DDLWorker & worker)
    {
        entry.parse(entry_data);
        entry_name = entry_name_;

        {
            const char * begin = entry.query.data();
            const char * end = begin + entry.query.size();

            ParserQuery parser_query(end);
            String description;
            query = parseQuery(parser_query, begin, end, description);
        }

        if (!query || !(query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query.get())))
            throw Exception("Recieved unknown DDL query", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

        cluster_name = query_on_cluster->cluster;
        cluster = worker.context.tryGetCluster(cluster_name);

        if (!cluster)
        {
            LOG_INFO(worker.log, "Will not execute entry " << entry_name << ": there is no cluster " << cluster_name << " on current host");
            return false;
        }

        bool found = false;
        const auto & shards = cluster->getShardsAddresses();
        for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
        {
            for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
            {
                const Cluster::Address & address = shards[shard_num][replica_num];

                if (isLocalAddress(address.resolved_address))
                {
                    if (found)
                    {
                        LOG_WARNING(worker.log, "There are at least two the same ClickHouse instances in cluster " << cluster_name << ": "
                            << host_id_in_cluster << " and " << address.toString()
                            << ". Will use the first one only.");
                    }
                    else
                    {
                        host_address = address;
                        host_id_in_cluster = address.toString();
                        host_shard_num = shard_num;
                        host_replica_num = replica_num;
                        found = true;
                    }
                }
            }
        }

        return found;
    }
};


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
    : context(context_)
{
    queue_dir = zk_root_dir;
    if (queue_dir.back() == '/')
        queue_dir.resize(queue_dir.size() - 1);

    host_fqdn = getFQDNOrHostName();
    host_fqdn_id = Cluster::Address::toString(host_fqdn, context.getTCPPort());

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
        const String & node_name = *it;
        String node_path = queue_dir + "/" + node_name;
        String node_data;

        if (!zookeeper->tryGet(node_path, node_data))
        {
            /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
            continue;
        }

        DDLTask task;
        bool found_cluster_and_host = false;
        try
        {
            found_cluster_and_host = task.fillFromEntryData(node_data, node_name, *this);
        }
        catch (...)
        {
            auto status = ExecutionStatus::fromCurrentException();
            /// We even cannot parse host name and can't properly submit execution status.
            /// What should we do?
        }

        const auto & hosts = task.entry.hosts;
        if (!found_cluster_and_host)
        {
            bool fqdn_in_hostlist = std::find(hosts.cbegin(), hosts.cend(), host_fqdn_id) != hosts.cend();
            if (fqdn_in_hostlist)
            {
                LOG_ERROR(log, "Not found current host in cluster " << task.cluster_name << " of task " << task.entry_name
                    << ", but found host " << host_fqdn_id << " with the same FQDN in host list of the task"
                    << ". Possibly inconsistent cluster definition among servers.");
            }
            else
            {
                LOG_DEBUG(log, "Skipping task " << node_data);
            }

            last_processed_node_name = node_name;
            continue;
        }
        else
        {
            bool host_in_hostlist = std::find(hosts.cbegin(), hosts.cend(), task.host_id_in_cluster) != hosts.cend();
            if (!host_in_hostlist)
            {
                LOG_ERROR(log, "Current host was found in cluster " << task.cluster_name
                    << ", but was not found in host list of task " << task.entry_name
                    << ". Possibly inconsistent cluster definition among servers.");

                last_processed_node_name = node_name;
                continue;
            }
        }

        bool already_processed = zookeeper->exists(node_path + "/finished/" + task.host_id_in_cluster);
        if (!server_startup && already_processed)
        {
            throw Exception(
                "Server expects that DDL task " + node_name + " should be processed, but it was already processed according to ZK",
                ErrorCodes::LOGICAL_ERROR);
        }

        if (!already_processed)
        {
            try
            {
                processTask(task);
            }
            catch (...)
            {
                tryLogCurrentException(log, "An error occurred while processing task " + node_name + " (" + task.entry.query + ")");
                throw;
            }
        }
        else
        {
            LOG_DEBUG(log, "Task " << node_name << " (" << task.entry.query << ") has been already processed");
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


void DDLWorker::processTask(DDLTask & task)
{
    LOG_DEBUG(log, "Processing entry " << task.entry_name << " (" << task.entry.query << ")");

    String node_path = queue_dir + "/" + task.entry_name;
    createStatusDirs(node_path);

    bool should_not_execute = current_node == task.entry_name && current_node_was_executed;

    if (!should_not_execute)
    {
        current_node = task.entry_name;
        current_node_was_executed = false;

        zookeeper->create(node_path + "/active/" + task.host_id_in_cluster, "", zkutil::CreateMode::Ephemeral);

        try
        {
            ASTPtr rewritten_ast = task.query_on_cluster->getRewrittenASTWithoutOnCluster(task.host_address.default_database);
            String rewritten_query = queryToString(rewritten_ast);

            LOG_DEBUG(log, "Executing query: " << rewritten_query);

            if (auto query_alter = dynamic_cast<const ASTAlterQuery *>(rewritten_ast.get()))
            {
                processTaskAlter(task, query_alter, rewritten_query, node_path);
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
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(node_path + "/active/" + task.host_id_in_cluster, -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/finished/" + task.host_id_in_cluster,
        current_node_execution_status.serializeText(), zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));

    int code = zookeeper->tryMultiWithRetries(ops);
    if (code != ZOK && code != ZNONODE)
    {
        /// FIXME: if server fails here, the task will be executed twice. We need WAL here.
        throw zkutil::KeeperException("Cannot commit executed entry " + task.entry_name, code);
    }
}


void DDLWorker::processTaskAlter(
    DDLTask & task,
    const ASTAlterQuery * query_alter,
    const String & rewritten_query,
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

    const auto & shard_info = task.cluster->getShardsInfo().at(task.host_shard_num);
    bool config_is_replicated_shard = shard_info.hasInternalReplication();

    if (execute_once_on_replica && !config_is_replicated_shard)
    {
        throw Exception("Table " + query_alter->table + " is replicated, but shard #" + toString(task.host_shard_num + 1) +
            " isn't replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }
    else if (!execute_once_on_replica && config_is_replicated_shard)
    {
        throw Exception("Table " + query_alter->table + " isn't replicated, but shard #" + toString(task.host_shard_num + 1) +
            " replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }

    if (execute_once_on_replica)
    {
        /// The following code can perform ALTER twice if
        ///  current server aquires lock, executes replicated alter,
        ///  losts zookeeper connection and doesn't have time to create /executed node, second server executes replicated alter again
        /// To avoid this problem alter() method of replicated tables should be changed and takes into account ddl query id tag.
        if (!context.getSettingsRef().distributed_ddl_allow_replicated_alter)
            throw Exception("Distributed DDL alters don't work properly yet", ErrorCodes::NOT_IMPLEMENTED);

        Strings replica_names;
        for (const auto & address : task.cluster->getShardsAddresses().at(task.host_shard_num))
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

            zkutil::Lock lock(zookeeper_holder, shard_path, "lock", task.host_id_in_cluster);
            std::mt19937 rng(std::hash<String>{}(task.host_id_in_cluster) + reinterpret_cast<intptr_t>(&rng));

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

                    zookeeper->create(is_executed_path, task.host_id_in_cluster, zkutil::CreateMode::Persistent);
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

            /// TODO: Add shared lock to avoid rare race conditions.

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
                throw Exception("Watching query is executing too long (" + toString(std::round(elapsed_seconds)) + " sec.)", ErrorCodes::TIMEOUT_EXCEEDED);

            if (num_hosts_finished != 0 || try_number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(20LU, try_number + 1)));

            /// TODO: add shared lock
            if (!zookeeper->exists(node_path))
            {
                throw Exception("Cannot provide query execution status. The query's node " + node_path
                                + " had been deleted by the cleaner since it was finished (or its lifetime is expired)",
                                ErrorCodes::UNFINISHED);
            }

            Strings new_hosts = getNewAndUpdate(finished_hosts_set, getChildrenAllowNoNode(zookeeper, node_path + "/finished"));
            ++try_number;
            if (new_hosts.empty())
                continue;

            Strings cur_active_hosts = getChildrenAllowNoNode(zookeeper, node_path + "/active");

            res = sample.cloneEmpty();
            for (const String & host_id : new_hosts)
            {
                ExecutionStatus status(1, "Cannot obtain error message");
                {
                    String status_data;
                    if (zookeeper->tryGet(node_path + "/finished/" + host_id, status_data))
                        status.deserializeText(status_data);
                }

                String host;
                UInt16 port;
                Cluster::Address::fromString(host_id, host, port);

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
    entry.initiator = ddl_worker.getCommonHostID();

    Cluster::AddressesWithFailover shards = cluster->getShardsAddresses();
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
