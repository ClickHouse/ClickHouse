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
#include <DataStreams/IProfilingBlockInputStream.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DNSCache.h>

#include <Common/getFQDNOrHostName.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/randomSeed.h>

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

#include <random>
#include <pcg_random.hpp>


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
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int UNFINISHED;
    extern const int UNKNOWN_STATUS_OF_DISTRIBUTED_DDL_TASK;
}


namespace
{

struct HostID
{
    String host_name;
    UInt16 port;

    HostID() = default;

    explicit HostID(const Cluster::Address & address)
    : host_name(address.host_name), port(address.port) {}

    static HostID fromString(const String & host_port_str)
    {
        HostID res;
        Cluster::Address::fromString(host_port_str, res.host_name, res.port);
        return res;
    }

    String toString() const
    {
        return Cluster::Address::toString(host_name, port);
    }

    String readableString() const
    {
        return host_name + ":" + DB::toString(port);
    }

    bool isLocalAddress(UInt16 clickhouse_port) const
    {
        try
        {
            return DB::isLocalAddress(Poco::Net::SocketAddress(host_name, port), clickhouse_port);
        }
        catch (const Poco::Exception & e)
        {
            /// Avoid "Host not found" exceptions
            return false;
        }
    }

    static String applyToString(const HostID & host_id)
    {
        return host_id.toString();
    }
};

}


struct DDLLogEntry
{
    String query;
    std::vector<HostID> hosts;
    String initiator; // optional

    static constexpr int CURRENT_VERSION = 1;

    String toString()
    {
        WriteBufferFromOwnString wb;

        Strings host_id_strings(hosts.size());
        std::transform(hosts.begin(), hosts.end(), host_id_strings.begin(), HostID::applyToString);

        auto version = CURRENT_VERSION;
        wb << "version: " << version << "\n";
        wb << "query: " << escape << query << "\n";
        wb << "hosts: " << host_id_strings << "\n";
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

        Strings host_id_strings;
        rb >> "query: " >> escape >> query >> "\n";
        rb >> "hosts: " >> host_id_strings >> "\n";

        if (!rb.eof())
            rb >> "initiator: " >> initiator >> "\n";
        else
            initiator.clear();

        assertEOF(rb);

        hosts.resize(host_id_strings.size());
        std::transform(host_id_strings.begin(), host_id_strings.end(), hosts.begin(), HostID::fromString);
    }
};


struct DDLTask
{
    /// Stages of task lifetime correspond ordering of these data fields:

    /// Stage 1: parse entry
    String entry_name;
    String entry_path;
    DDLLogEntry entry;

    /// Stage 2: resolve host_id and check that
    HostID host_id;
    String host_id_str;

    /// Stage 3.1: parse query
    ASTPtr query;
    ASTQueryWithOnCluster * query_on_cluster = nullptr;

    /// Stage 3.2: check cluster and find the host in cluster
    String cluster_name;
    ClusterPtr cluster;
    Cluster::Address address_in_cluster;
    size_t host_shard_num;
    size_t host_replica_num;

    /// Stage 3.3: execute query
    ExecutionStatus execution_status;
    bool was_executed = false;

    /// Stage 4: commit results to ZooKeeper
};


static std::unique_ptr<zkutil::Lock> createSimpleZooKeeperLock(
    std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & lock_prefix, const String & lock_name, const String & lock_message)
{
    auto zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
    zookeeper_holder->initFromInstance(zookeeper);
    return std::make_unique<zkutil::Lock>(std::move(zookeeper_holder), lock_prefix, lock_name, lock_message);
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

    return supported_alter_types.count(type) != 0;
}


DDLWorker::DDLWorker(const std::string & zk_root_dir, Context & context_, const Poco::Util::AbstractConfiguration * config, const String & prefix)
    : context(context_), log(&Logger::get("DDLWorker"))
{
    queue_dir = zk_root_dir;
    if (queue_dir.back() == '/')
        queue_dir.resize(queue_dir.size() - 1);

    if (config)
    {
        task_max_lifetime = config->getUInt64(prefix + ".task_max_lifetime", static_cast<UInt64>(task_max_lifetime));
        cleanup_delay_period = config->getUInt64(prefix + ".cleanup_delay_period", static_cast<UInt64>(cleanup_delay_period));
        max_tasks_in_queue = std::max(static_cast<UInt64>(1), config->getUInt64(prefix + ".max_tasks_in_queue", max_tasks_in_queue));
    }

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


bool DDLWorker::initAndCheckTask(const String & entry_name, String & out_reason)
{
    String node_data;
    String entry_path = queue_dir + "/" + entry_name;

    if (!zookeeper->tryGet(entry_path, node_data))
    {
        /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
        out_reason = "The task was deleted";
        return false;
    }

    auto task = std::make_unique<DDLTask>();
    task->entry_name = entry_name;
    task->entry_path = entry_path;

    try
    {
        task->entry.parse(node_data);
    }
    catch (...)
    {
        /// What should we do if we even cannot parse host name and therefore cannot properly submit execution status?
        /// We can try to create fail node using FQDN if it equal to host name in cluster config attempt will be sucessfull.
        /// Otherwise, that node will be ignored by DDLQueryStatusInputSream.

        tryLogCurrentException(log, "Cannot parse DDL task " + entry_name + ", will try to send error status");

        String status = ExecutionStatus::fromCurrentException().serializeText();
        try
        {
            createStatusDirs(entry_path);
            zookeeper->tryCreate(entry_path + "/finished/" + host_fqdn_id, status, zkutil::CreateMode::Persistent);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Can't report the task has invalid format");
        }

        out_reason = "Incorrect task format";
        return false;
    }

    bool host_in_hostlist = false;
    for (const HostID & host : task->entry.hosts)
    {
        if (!host.isLocalAddress(context.getTCPPort()))
            continue;

        if (host_in_hostlist)
        {
            /// This check could be slow a little bit
            LOG_WARNING(log, "There are two the same ClickHouse instances in task " << entry_name
                << ": " << task->host_id.readableString() << " and " << host.readableString() << ". Will use the first one only.");
        }
        else
        {
            host_in_hostlist = true;
            task->host_id = host;
            task->host_id_str = host.toString();
        }
    }

    if (host_in_hostlist)
        current_task = std::move(task);
    else
        out_reason = "There is no a local address in host list";

    return host_in_hostlist;
}


static void filterAndSortQueueNodes(Strings & all_nodes)
{
    all_nodes.erase(std::remove_if(all_nodes.begin(), all_nodes.end(), [] (const String & s) { return !startsWith(s, "query-"); }), all_nodes.end());
    std::sort(all_nodes.begin(), all_nodes.end());
}


void DDLWorker::processTasks()
{
    LOG_DEBUG(log, "Processing tasks");

    Strings queue_nodes = zookeeper->getChildren(queue_dir, nullptr, event_queue_updated);
    filterAndSortQueueNodes(queue_nodes);
    if (queue_nodes.empty())
        return;

    bool server_startup = last_processed_task_name.empty();

    auto begin_node = server_startup
        ? queue_nodes.begin()
        : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_processed_task_name);

    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String entry_name = *it;

        if (current_task)
        {
            if (current_task->entry_name == entry_name)
            {
                LOG_INFO(log, "Trying to process task " << entry_name << " again");
            }
            else
            {
                LOG_INFO(log, "Task " << current_task->entry_name << " was deleted from ZooKeeper before current host committed it");
                current_task = nullptr;
            }
        }

        if (!current_task)
        {
            String reason;
            if (!initAndCheckTask(entry_name, reason))
            {
                LOG_DEBUG(log, "Will not execute task " << entry_name << " : " << reason);
                last_processed_task_name = entry_name;
                continue;
            }
        }

        DDLTask & task = *current_task;

        bool already_processed = zookeeper->exists(task.entry_path + "/finished/" + task.host_id_str);
        if (!server_startup && !task.was_executed && already_processed)
        {
            throw Exception(
                "Server expects that DDL task " + task.entry_name + " should be processed, but it was already processed according to ZK",
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
                LOG_WARNING(log, "An error occurred while processing task " << task.entry_name << " (" << task.entry.query << ") : "
                    << getCurrentExceptionMessage(true));
                throw;
            }
        }
        else
        {
            LOG_DEBUG(log, "Task " << task.entry_name << " (" << task.entry.query << ") has been already processed");
        }

        last_processed_task_name = task.entry_name;
        current_task.reset();

        if (stop_flag)
            break;
    }
}


/// Parses query and resolves cluster and host in cluster
void DDLWorker::parseQueryAndResolveHost(DDLTask & task)
{
    {
        const char * begin = task.entry.query.data();
        const char * end = begin + task.entry.query.size();

        ParserQuery parser_query(end);
        String description;
        task.query = parseQuery(parser_query, begin, end, description);
    }

    if (!task.query || !(task.query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(task.query.get())))
        throw Exception("Recieved unknown DDL query", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

    task.cluster_name = task.query_on_cluster->cluster;
    task.cluster = context.tryGetCluster(task.cluster_name);
    if (!task.cluster)
    {
        throw Exception("DDL task " + task.entry_name + " contains current host " + task.host_id.readableString()
            + " in cluster " + task.cluster_name + ", but there are no such cluster here.", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }

    /// Try to find host from task host list in cluster
    /// At the first, try find exact match (host name and ports should be literally equal)
    /// If the attempt fails, try find it resolving host name of each instance
    const auto & shards = task.cluster->getShardsAddresses();

    bool found_exact_match = false;
    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (address.host_name == task.host_id.host_name && address.port == task.host_id.port)
            {
                if (found_exact_match)
                {
                    throw Exception("There are two exactly the same ClickHouse instances " + address.readableString()
                        + " in cluster " + task.cluster_name, ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
                }

                found_exact_match = true;
                task.host_shard_num = shard_num;
                task.host_replica_num = replica_num;
                task.address_in_cluster = address;
            }
        }
    }

    if (found_exact_match)
        return;

    LOG_WARNING(log, "Not found the exact match of host " << task.host_id.readableString() << " from task " << task.entry_name
        << " in cluster " << task.cluster_name << " definition. Will try to find it using host name resolving.");

    bool found_via_resolving = false;
    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (isLocalAddress(address.resolved_address, context.getTCPPort()))
            {
                if (found_via_resolving)
                {
                    throw Exception("There are two the same ClickHouse instances in cluster " + task.cluster_name + " : "
                        + task.address_in_cluster.readableString() + " and " + address.readableString(), ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
                }
                else
                {
                    found_via_resolving = true;
                    task.host_shard_num = shard_num;
                    task.host_replica_num = replica_num;
                    task.address_in_cluster = address;
                }
            }
        }
    }

    if (!found_via_resolving)
    {
        throw Exception("Not found host " + task.host_id.readableString() + " in definition of cluster " + task.cluster_name,
                        ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }
    else
    {
        LOG_INFO(log, "Resolved host " << task.host_id.readableString() << " from task " << task.entry_name
            << " as host " << task.address_in_cluster.readableString() << " in definition of cluster " << task.cluster_name);
    }
}


bool DDLWorker::tryExecuteQuery(const String & query, const DDLTask & task, ExecutionStatus & status)
{
    /// Add special comment at the start of query to easily identify DDL-produced queries in query_log
    String query_prefix = "/* ddl_entry=" + task.entry_name + " */ ";
    String query_to_execute = query_prefix + query;

    ReadBufferFromString istr(query_to_execute);
    String dummy_string;
    WriteBufferFromString ostr(dummy_string);

    try
    {
        Context local_context(context);
        executeQuery(istr, ostr, false, local_context, nullptr);
    }
    catch (...)
    {
        status = ExecutionStatus::fromCurrentException();
        tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");

        return false;
    }

    status = ExecutionStatus(0);
    LOG_DEBUG(log, "Executed query: " << query);

    return true;
}


void DDLWorker::processTask(DDLTask & task)
{
    LOG_DEBUG(log, "Processing task " << task.entry_name << " (" << task.entry.query << ")");

    String dummy;
    String active_node_path = task.entry_path + "/active/" + task.host_id_str;
    String finished_node_path = task.entry_path + "/finished/" + task.host_id_str;

    auto code = zookeeper->tryCreateWithRetries(active_node_path, "", zkutil::CreateMode::Ephemeral, dummy);
    if (code == ZOK || code == ZNODEEXISTS)
    {
        // Ok
    }
    else if (code == ZNONODE)
    {
        /// There is no parent
        createStatusDirs(task.entry_path);
        if (ZOK != zookeeper->tryCreateWithRetries(active_node_path, "", zkutil::CreateMode::Ephemeral, dummy))
            throw zkutil::KeeperException(code, active_node_path);
    }
    else
        throw zkutil::KeeperException(code, active_node_path);

    if (!task.was_executed)
    {
        try
        {
            parseQueryAndResolveHost(task);

            ASTPtr rewritten_ast = task.query_on_cluster->getRewrittenASTWithoutOnCluster(task.address_in_cluster.default_database);
            String rewritten_query = queryToString(rewritten_ast);
            LOG_DEBUG(log, "Executing query: " << rewritten_query);

            if (auto ast_alter = dynamic_cast<const ASTAlterQuery *>(rewritten_ast.get()))
            {
                processTaskAlter(task, ast_alter, rewritten_query, task.entry_path);
            }
            else
            {
                tryExecuteQuery(rewritten_query, task, task.execution_status);
            }
        }
        catch (const zkutil::KeeperException & e)
        {
            throw;
        }
        catch (...)
        {
            task.execution_status = ExecutionStatus::fromCurrentException("An error occured before execution");
        }

        /// We need to distinguish ZK errors occured before and after query executing
        task.was_executed = true;
    }

    /// FIXME: if server fails right here, the task will be executed twice. We need WAL here.

    /// Delete active flag and create finish flag
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(active_node_path, -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(finished_node_path, task.execution_status.serializeText(),
                                                          zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);
}


void DDLWorker::processTaskAlter(
    DDLTask & task,
    const ASTAlterQuery * ast_alter,
    const String & rewritten_query,
    const String & node_path)
{
    String database = ast_alter->database.empty() ? context.getCurrentDatabase() : ast_alter->database;
    StoragePtr storage = context.getTable(database, ast_alter->table);

    bool execute_once_on_replica = storage->supportsReplication();
    bool execute_on_leader_replica = false;

    for (const auto & param : ast_alter->parameters)
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
        throw Exception("Table " + ast_alter->table + " is replicated, but shard #" + toString(task.host_shard_num + 1) +
            " isn't replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }
    else if (!execute_once_on_replica && config_is_replicated_shard)
    {
        throw Exception("Table " + ast_alter->table + " isn't replicated, but shard #" + toString(task.host_shard_num + 1) +
            " is replicated according to its cluster definition", ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION);
    }

    if (execute_once_on_replica)
    {
        /// The following code can perform ALTER twice if:
        ///  current server acquires the lock, executes replicated alter,
        ///  loses zookeeper connection and doesn't have time to create /executed node, second server executes replicated alter again
        /// To avoid this problem alter() method of replicated tables should be changed and takes into account ddl query id tag.
        if (!context.getSettingsRef().distributed_ddl_allow_replicated_alter)
            throw Exception("Distributed DDL alters for replicated tables don't work properly yet", ErrorCodes::NOT_IMPLEMENTED);

        /// Generate unique name for shard node, it will be used to execute the query by only single host
        /// Shard node name has format 'replica_name1,replica_name2,...,replica_nameN'
        /// Where replica_name is 'escape(replica_ip_address):replica_port'
        /// FIXME: this replica_name could be changed after replica restart
        Strings replica_names;
        for (const Cluster::Address & address : task.cluster->getShardsAddresses().at(task.host_shard_num))
            replica_names.emplace_back(address.resolved_address.host().toString());
        std::sort(replica_names.begin(), replica_names.end());

        String shard_node_name;
        for (auto it = replica_names.begin(); it != replica_names.end(); ++it)
            shard_node_name += *it + (std::next(it) != replica_names.end() ? "," : "");

        String shard_path = node_path + "/shards/" + shard_node_name;
        String is_executed_path = shard_path + "/executed";
        zookeeper->createAncestors(shard_path + "/");

        bool alter_executed_by_any_replica = false;
        {
            auto lock = createSimpleZooKeeperLock(zookeeper, shard_path, "lock", task.host_id_str);
            pcg64 rng(randomSeed());

            for (int num_tries = 0; num_tries < 10; ++num_tries)
            {
                if (zookeeper->exists(is_executed_path))
                {
                    alter_executed_by_any_replica = true;
                    break;
                }

                if (lock->tryLock())
                {
                    tryExecuteQuery(rewritten_query, task, task.execution_status);

                    if (execute_on_leader_replica && task.execution_status.code == ErrorCodes::NOT_IMPLEMENTED)
                    {
                        /// TODO: it is ok to receive exception "host is not leader"
                    }

                    zookeeper->create(is_executed_path, task.host_id_str, zkutil::CreateMode::Persistent);
                    lock->unlock();
                    alter_executed_by_any_replica = true;
                    break;
                }

                std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, 1)(rng)));
            }
        }

        if (!alter_executed_by_any_replica)
            task.execution_status = ExecutionStatus(ErrorCodes::NOT_IMPLEMENTED, "Cannot enqueue replicated DDL query");
    }
    else
    {
        tryExecuteQuery(rewritten_query, task, task.execution_status);
    }
}


void DDLWorker::cleanupQueue()
{
    /// Both ZK and Poco use Unix epoch
    Int64 current_time_seconds = Poco::Timestamp().epochTime();
    constexpr UInt64 zookeeper_time_resolution = 1000;

    /// Too early to check
    if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_delay_period)
        return;

    last_cleanup_time_seconds = current_time_seconds;

    LOG_DEBUG(log, "Cleaning queue");

    Strings queue_nodes = zookeeper->getChildren(queue_dir);
    filterAndSortQueueNodes(queue_nodes);

    size_t num_outdated_nodes = (queue_nodes.size() > max_tasks_in_queue) ? queue_nodes.size() - max_tasks_in_queue : 0;
    auto first_non_outdated_node = queue_nodes.begin() + num_outdated_nodes;

    for (auto it = queue_nodes.cbegin(); it < queue_nodes.cend(); ++it)
    {
        String node_name = *it;
        String node_path = queue_dir + "/" + node_name;
        String lock_path = node_path + "/lock";

        zkutil::Stat stat;
        String dummy;

        try
        {
            /// Already deleted
            if (!zookeeper->exists(node_path, &stat))
                continue;

            /// Delete node if its lifetmie is expired (according to task_max_lifetime parameter)
            Int64 zookeeper_time_seconds = stat.ctime / zookeeper_time_resolution;
            bool node_lifetime_is_expired = zookeeper_time_seconds + task_max_lifetime < current_time_seconds;

            /// If too many nodes in task queue (> max_tasks_in_queue), delete oldest one
            bool node_is_outside_max_window = it < first_non_outdated_node;

            if (!node_lifetime_is_expired && !node_is_outside_max_window)
                continue;

            /// Skip if there are active nodes (it is weak guard)
            if (zookeeper->exists(node_path + "/active", &stat) && stat.numChildren > 0)
            {
                LOG_INFO(log, "Task " << node_name << " should be deleted, but there are active workers. Skipping it.");
                continue;
            }

            /// Usage of the lock is not necessary now (tryRemoveRecursive correctly removes node in a presence of concurrent cleaners)
            /// But the lock will be required to implement system.distributed_ddl_queue table
            auto lock = createSimpleZooKeeperLock(zookeeper, node_path, "lock", host_fqdn_id);
            if (!lock->tryLock())
            {
                LOG_INFO(log, "Task " << node_name << " should be deleted, but it is locked. Skipping it.");
                continue;
            }

            if (node_lifetime_is_expired)
                LOG_INFO(log, "Lifetime of task " << node_name << " is expired, deleting it");
            else if (node_is_outside_max_window)
                LOG_INFO(log, "Task " << node_name << " is outdated, deleting it");

            /// Deleting
            {
                Strings childs = zookeeper->getChildren(node_path);
                for (const String & child : childs)
                {
                    if (child != "lock")
                        zookeeper->tryRemoveRecursive(node_path + "/" + child);
                }

                /// Remove the lock node and its parent atomically
                zkutil::Ops ops;
                ops.emplace_back(std::make_unique<zkutil::Op::Remove>(lock_path, -1));
                ops.emplace_back(std::make_unique<zkutil::Op::Remove>(node_path, -1));
                zookeeper->multi(ops);

                lock->unlockAssumeLockNodeRemovedManually();
            }
        }
        catch (...)
        {
            LOG_INFO(log, "An error occured while checking and cleaning task " + node_name + " from queue: " + getCurrentExceptionMessage(false));
        }
    }
}


/// Try to create nonexisting "status" dirs for a node
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
        throw Exception("Empty host list in a distributed DDL task", ErrorCodes::LOGICAL_ERROR);

    String query_path_prefix = queue_dir + "/query-";
    zookeeper->createAncestors(query_path_prefix);

    String node_path;
    try
    {
        node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);
    }
    catch (const zkutil::KeeperException & e)
    {
        /// TODO: This condition could be relaxed with additional post-checks
        if (e.isTemporaryError())
            throw Exception("Unknown status of distributed DDL task", ErrorCodes::UNKNOWN_STATUS_OF_DISTRIBUTED_DDL_TASK);

        throw;
    }

    /// Optional step
    try
    {
        createStatusDirs(node_path);
    }
    catch (...)
    {
        LOG_INFO(log, "An error occurred while creating auxiliary ZooKeeper directories in " << node_path << " . They will be created later"
            << ". Error : " << getCurrentExceptionMessage(true));
    }

    return node_path;
}


void DDLWorker::run()
{
    setThreadName("DDLWorker");
    LOG_DEBUG(log, "Started DDLWorker thread");

    bool initialized = false;
    do
    {
        try
        {
            try
            {
                zookeeper = context.getZooKeeper();
                zookeeper->createAncestors(queue_dir + "/");
                initialized = true;
            }
            catch (const zkutil::KeeperException & e)
            {
                if (!e.isHardwareError())
                    throw;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Terminating. Cannot initialize DDL queue.");
            return;
        }
    } while (!initialized && !stop_flag);

    while (!stop_flag)
    {
        try
        {
            processTasks();

            LOG_DEBUG(log, "Waiting a watch");
            event_queue_updated->wait();

            if (stop_flag)
                break;

            /// TODO: it might delay the execution, move it to separate thread.
            cleanupQueue();
        }
        catch (zkutil::KeeperException & e)
        {
            if (e.isHardwareError())
            {
                if (!e.isTemporaryError())
                {
                    LOG_DEBUG(log, "Recovering ZooKeeper session after: " << getCurrentExceptionMessage(false));

                    while (!stop_flag)
                    {
                        try
                        {
                            zookeeper = context.getZooKeeper();
                            break;
                        }
                        catch (...)
                        {
                            tryLogCurrentException(__PRETTY_FUNCTION__);

                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(5s);
                        }
                    }
                }
                else
                {
                    LOG_DEBUG(log, "Retry task processing after: " << getCurrentExceptionMessage(false));
                }
            }
            else
            {
                LOG_ERROR(log, "Unexpected ZooKeeper error: " << getCurrentExceptionMessage(true) << ". Terminating.");
                return;
            }

            /// Unlock the processing just in case
            event_queue_updated->set();
        }
        catch (...)
        {
            LOG_ERROR(log, "Unexpected error: " << getCurrentExceptionMessage(true) << ". Terminating.");
            return;
        }
    }
}


class DDLQueryStatusInputSream : public IProfilingBlockInputStream
{
public:

    DDLQueryStatusInputSream(const String & zk_node_path, const DDLLogEntry & entry, const Context & context)
    : node_path(zk_node_path), context(context), watch(CLOCK_MONOTONIC_COARSE), log(&Logger::get("DDLQueryStatusInputSream"))
    {
        sample = Block{
            {std::make_shared<DataTypeString>(),    "host"},
            {std::make_shared<DataTypeUInt16>(),    "port"},
            {std::make_shared<DataTypeInt64>(),     "status"},
            {std::make_shared<DataTypeString>(),    "error"},
            {std::make_shared<DataTypeUInt64>(),    "num_hosts_remaining"},
            {std::make_shared<DataTypeUInt64>(),    "num_hosts_active"},
        };

        for (const HostID & host: entry.hosts)
            waiting_hosts.emplace(host.toString());

        setTotalRowsApprox(entry.hosts.size());

        timeout_seconds = context.getSettingsRef().distributed_ddl_task_timeout;
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
        if (num_hosts_finished >= waiting_hosts.size())
            return res;

        auto zookeeper = context.getZooKeeper();
        size_t try_number = 0;

        while(res.rows() == 0)
        {
            if (is_cancelled)
                return res;

            auto elapsed_seconds = watch.elapsedSeconds();
            if (timeout_seconds >= 0 && elapsed_seconds > timeout_seconds)
            {
                throw Exception("Watching task " + node_path + " is executing too long (" + toString(std::round(elapsed_seconds)) + " sec.)",
                                ErrorCodes::TIMEOUT_EXCEEDED);
            }

            if (num_hosts_finished != 0 || try_number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(static_cast<size_t>(20), try_number + 1)));

            /// TODO: add shared lock
            if (!zookeeper->exists(node_path))
            {
                throw Exception("Cannot provide query execution status. The query's node " + node_path
                                + " had been deleted by the cleaner since it was finished (or its lifetime is expired)",
                                ErrorCodes::UNFINISHED);
            }

            Strings new_hosts = getNewAndUpdate(getChildrenAllowNoNode(zookeeper, node_path + "/finished"));
            ++try_number;
            if (new_hosts.empty())
                continue;

            Strings cur_active_hosts = getChildrenAllowNoNode(zookeeper, node_path + "/active");

            res = sample.cloneEmpty();
            for (const String & host_id : new_hosts)
            {
                ExecutionStatus status(-1, "Cannot obtain error message");
                {
                    String status_data;
                    if (zookeeper->tryGet(node_path + "/finished/" + host_id, status_data))
                        status.tryDeserializeText(status_data);
                }

                String host;
                UInt16 port;
                Cluster::Address::fromString(host_id, host, port);

                res.getByName("host").column->insert(host);
                res.getByName("port").column->insert(static_cast<UInt64>(port));
                res.getByName("status").column->insert(static_cast<Int64>(status.code));
                res.getByName("error").column->insert(status.message);
                res.getByName("num_hosts_remaining").column->insert(static_cast<UInt64>(waiting_hosts.size() - (++num_hosts_finished)));
                res.getByName("num_hosts_active").column->insert(static_cast<UInt64>(cur_active_hosts.size()));
            }
        }

        return res;
    }

    Block getSampleBlock() const
    {
        return sample.cloneEmpty();
    }

    ~DDLQueryStatusInputSream() override = default;

private:

    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
    {
        Strings res;
        int code = zookeeper->tryGetChildren(node_path, res);
        if (code != ZOK && code != ZNONODE)
            throw zkutil::KeeperException(code, node_path);
        return res;
    }

    Strings getNewAndUpdate(const Strings & current_list_of_finished_hosts)
    {
        Strings diff;
        for (const String & host : current_list_of_finished_hosts)
        {
            if (!waiting_hosts.count(host))
            {
                if (!ignoring_hosts.count(host))
                {
                    ignoring_hosts.emplace(host);
                    LOG_INFO(log, "Unexpected host " << host << " appeared " << " in task " << node_path);
                }
                continue;
            }

            if (!finished_hosts.count(host))
            {
                diff.emplace_back(host);
                finished_hosts.emplace(host);
            }
        }

        return diff;
    }

private:
    String node_path;
    const Context & context;
    Stopwatch watch;
    Logger * log;

    Block sample;

    NameSet waiting_hosts;  /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    size_t num_hosts_finished = 0;

    Int64 timeout_seconds = 120;
};


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, const Context & context)
{
    ASTPtr query_ptr;

    /// Remove FORMAT ... INTO OUTFILE if exists
    if (dynamic_cast<const ASTQueryWithOutput *>(query_ptr_.get()))
    {
        query_ptr = query_ptr_->clone();
        auto query_with_output = dynamic_cast<ASTQueryWithOutput *>(query_ptr.get());
        query_with_output->out_file = nullptr;
        query_with_output->format = nullptr;
    }
    else
        query_ptr = query_ptr_;

    auto query = dynamic_cast<const ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Distributed execution is not supported for such DDL queries", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (auto query_alter = dynamic_cast<const ASTAlterQuery *>(query_ptr.get()))
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
            entry.hosts.emplace_back(addr);
    }

    String node_path = ddl_worker.enqueueQuery(entry);

    BlockIO io;
    if (context.getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    auto stream = std::make_shared<DDLQueryStatusInputSream>(node_path, entry, context);
    io.in_sample = stream->getSampleBlock();
    io.in = std::move(stream);
    return io;
}


}
