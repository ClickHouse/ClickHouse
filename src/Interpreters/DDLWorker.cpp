#include <Interpreters/DDLWorker.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Access/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Common/DNSResolver.h>
#include <Common/Macros.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/isLocalAddress.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Poco/Timestamp.h>
#include <Poco/Net/NetException.h>
#include <common/sleep.h>
#include <common/getFQDNOrHostName.h>
#include <random>
#include <pcg_random.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int UNFINISHED;
    extern const int QUERY_IS_PROHIBITED;
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
        std::tie(res.host_name, res.port) = Cluster::Address::fromString(host_port_str);
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
            return DB::isLocalAddress(DNSResolver::instance().resolveAddress(host_name, port), clickhouse_port);
        }
        catch (const Poco::Net::NetException &)
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
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown DDLLogEntry format version: {}", version);

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


namespace
{

/** Caveats: usage of locks in ZooKeeper is incorrect in 99% of cases,
  *  and highlights your poor understanding of distributed systems.
  *
  * It's only correct if all the operations that are performed under lock
  *  are atomically checking that the lock still holds
  *  or if we ensure that these operations will be undone if lock is lost
  *  (due to ZooKeeper session loss) that's very difficult to achieve.
  *
  * It's Ok if every operation that we perform under lock is actually operation in ZooKeeper.
  *
  * In 1% of cases when you can correctly use Lock, the logic is complex enough, so you don't need this class.
  *
  * TLDR: Don't use this code.
  * We only have a few cases of it's usage and it will be removed.
  */
class ZooKeeperLock
{
public:
    /// lock_prefix - path where the ephemeral lock node will be created
    /// lock_name - the name of the ephemeral lock node
    ZooKeeperLock(
        const zkutil::ZooKeeperPtr & zookeeper_,
        const std::string & lock_prefix_,
        const std::string & lock_name_,
        const std::string & lock_message_ = "")
    :
        zookeeper(zookeeper_),
        lock_path(lock_prefix_ + "/" + lock_name_),
        lock_message(lock_message_),
        log(&Poco::Logger::get("zkutil::Lock"))
    {
        zookeeper->createIfNotExists(lock_prefix_, "");
    }

    ~ZooKeeperLock()
    {
        try
        {
            unlock();
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void unlock()
    {
        Coordination::Stat stat;
        std::string dummy;
        bool result = zookeeper->tryGet(lock_path, dummy, &stat);

        if (result && stat.ephemeralOwner == zookeeper->getClientID())
            zookeeper->remove(lock_path, -1);
        else
            LOG_WARNING(log, "Lock is lost. It is normal if session was expired. Path: {}/{}", lock_path, lock_message);
    }

    bool tryLock()
    {
        std::string dummy;
        Coordination::Error code = zookeeper->tryCreate(lock_path, lock_message, zkutil::CreateMode::Ephemeral, dummy);

        if (code == Coordination::Error::ZNODEEXISTS)
        {
            return false;
        }
        else if (code == Coordination::Error::ZOK)
        {
            return true;
        }
        else
        {
            throw Coordination::Exception(code);
        }
    }

private:
    zkutil::ZooKeeperPtr zookeeper;

    std::string lock_path;
    std::string lock_message;
    Poco::Logger * log;

};

std::unique_ptr<ZooKeeperLock> createSimpleZooKeeperLock(
    const zkutil::ZooKeeperPtr & zookeeper, const String & lock_prefix, const String & lock_name, const String & lock_message)
{
    return std::make_unique<ZooKeeperLock>(zookeeper, lock_prefix, lock_name, lock_message);
}

}


static bool isSupportedAlterType(int type)
{
    static const std::unordered_set<int> unsupported_alter_types{
        ASTAlterCommand::ATTACH_PARTITION,
        ASTAlterCommand::REPLACE_PARTITION,
        ASTAlterCommand::FETCH_PARTITION,
        ASTAlterCommand::FREEZE_PARTITION,
        ASTAlterCommand::FREEZE_ALL,
        ASTAlterCommand::NO_TYPE,
    };

    return unsupported_alter_types.count(type) == 0;
}


DDLWorker::DDLWorker(int pool_size_, const std::string & zk_root_dir, Context & context_, const Poco::Util::AbstractConfiguration * config, const String & prefix)
    : context(context_)
    , log(&Poco::Logger::get("DDLWorker"))
    , pool_size(pool_size_)
    , worker_pool(pool_size_)
{
    last_tasks.reserve(pool_size);

    queue_dir = zk_root_dir;
    if (queue_dir.back() == '/')
        queue_dir.resize(queue_dir.size() - 1);

    if (config)
    {
        task_max_lifetime = config->getUInt64(prefix + ".task_max_lifetime", static_cast<UInt64>(task_max_lifetime));
        cleanup_delay_period = config->getUInt64(prefix + ".cleanup_delay_period", static_cast<UInt64>(cleanup_delay_period));
        max_tasks_in_queue = std::max<UInt64>(1, config->getUInt64(prefix + ".max_tasks_in_queue", max_tasks_in_queue));

        if (config->has(prefix + ".profile"))
            context.setSetting("profile", config->getString(prefix + ".profile"));
    }

    if (context.getSettingsRef().readonly)
    {
        LOG_WARNING(log, "Distributed DDL worker is run with readonly settings, it will not be able to execute DDL queries Set appropriate system_profile or distributed_ddl.profile to fix this.");
    }

    host_fqdn = getFQDNOrHostName();
    host_fqdn_id = Cluster::Address::toString(host_fqdn, context.getTCPPort());

    main_thread = ThreadFromGlobalPool(&DDLWorker::runMainThread, this);
    cleanup_thread = ThreadFromGlobalPool(&DDLWorker::runCleanupThread, this);
}


DDLWorker::~DDLWorker()
{
    stop_flag = true;
    queue_updated_event->set();
    cleanup_event->set();
    worker_pool.wait();
    main_thread.join();
    cleanup_thread.join();
}


DDLWorker::ZooKeeperPtr DDLWorker::tryGetZooKeeper() const
{
    std::lock_guard lock(zookeeper_mutex);
    return current_zookeeper;
}

DDLWorker::ZooKeeperPtr DDLWorker::getAndSetZooKeeper()
{
    std::lock_guard lock(zookeeper_mutex);

    if (!current_zookeeper || current_zookeeper->expired())
        current_zookeeper = context.getZooKeeper();

    return current_zookeeper;
}

void DDLWorker::recoverZooKeeper()
{
    LOG_DEBUG(log, "Recovering ZooKeeper session after: {}", getCurrentExceptionMessage(false));

    while (!stop_flag)
    {
        try
        {
            getAndSetZooKeeper();
            break;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            sleepForSeconds(5);
        }
    }
}


DDLTaskPtr DDLWorker::initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper)
{
    String node_data;
    String entry_path = queue_dir + "/" + entry_name;

    if (!zookeeper->tryGet(entry_path, node_data))
    {
        /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
        out_reason = "The task was deleted";
        return {};
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
        /// We can try to create fail node using FQDN if it equal to host name in cluster config attempt will be successful.
        /// Otherwise, that node will be ignored by DDLQueryStatusInputStream.

        tryLogCurrentException(log, "Cannot parse DDL task " + entry_name + ", will try to send error status");

        String status = ExecutionStatus::fromCurrentException().serializeText();
        try
        {
            createStatusDirs(entry_path, zookeeper);
            zookeeper->tryCreate(entry_path + "/finished/" + host_fqdn_id, status, zkutil::CreateMode::Persistent);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Can't report the task has invalid format");
        }

        out_reason = "Incorrect task format";
        return {};
    }

    bool host_in_hostlist = false;
    for (const HostID & host : task->entry.hosts)
    {
        auto maybe_secure_port = context.getTCPPortSecure();

        /// The port is considered local if it matches TCP or TCP secure port that the server is listening.
        bool is_local_port = (maybe_secure_port && host.isLocalAddress(*maybe_secure_port))
            || host.isLocalAddress(context.getTCPPort());

        if (!is_local_port)
            continue;

        if (host_in_hostlist)
        {
            /// This check could be slow a little bit
            LOG_WARNING(log, "There are two the same ClickHouse instances in task {}: {} and {}. Will use the first one only.", entry_name, task->host_id.readableString(), host.readableString());
        }
        else
        {
            host_in_hostlist = true;
            task->host_id = host;
            task->host_id_str = host.toString();
        }
    }

    if (!host_in_hostlist)
    {
        out_reason = "There is no a local address in host list";
        return {};
    }

    return task;
}


static void filterAndSortQueueNodes(Strings & all_nodes)
{
    all_nodes.erase(std::remove_if(all_nodes.begin(), all_nodes.end(), [] (const String & s) { return !startsWith(s, "query-"); }), all_nodes.end());
    std::sort(all_nodes.begin(), all_nodes.end());
}

void DDLWorker::scheduleTasks()
{
    LOG_DEBUG(log, "Scheduling tasks");
    auto zookeeper = tryGetZooKeeper();

    Strings queue_nodes = zookeeper->getChildren(queue_dir, nullptr, queue_updated_event);
    filterAndSortQueueNodes(queue_nodes);
    if (queue_nodes.empty())
        return;

    bool server_startup = last_tasks.empty();

    auto begin_node = server_startup
        ? queue_nodes.begin()
        : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_tasks.back());

    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String entry_name = *it;

        String reason;
        auto task = initAndCheckTask(entry_name, reason, zookeeper);
        if (!task)
        {
            LOG_DEBUG(log, "Will not execute task {}: {}", entry_name, reason);
            saveTask(entry_name);
            continue;
        }

        bool already_processed = zookeeper->exists(task->entry_path + "/finished/" + task->host_id_str);
        if (!server_startup && !task->was_executed && already_processed)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Server expects that DDL task {} should be processed, but it was already processed according to ZK",
                entry_name);
        }

        if (!already_processed)
        {
            worker_pool.scheduleOrThrowOnError([this, task_ptr = task.release()]()
            {
                setThreadName("DDLWorkerExec");
                enqueueTask(DDLTaskPtr(task_ptr));
            });
        }
        else
        {
            LOG_DEBUG(log, "Task {} ({}) has been already processed", entry_name, task->entry.query);
        }

        saveTask(entry_name);

        if (stop_flag)
            break;
    }
}

void DDLWorker::saveTask(const String & entry_name)
{
    if (last_tasks.size() == pool_size)
    {
        last_tasks.erase(last_tasks.begin());
    }
    last_tasks.emplace_back(entry_name);
}

/// Parses query and resolves cluster and host in cluster
void DDLWorker::parseQueryAndResolveHost(DDLTask & task)
{
    {
        const char * begin = task.entry.query.data();
        const char * end = begin + task.entry.query.size();

        ParserQuery parser_query(end);
        String description;
        task.query = parseQuery(parser_query, begin, end, description, 0, context.getSettingsRef().max_parser_depth);
    }

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    if (!task.query || !(task.query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(task.query.get())))
        throw Exception("Received unknown DDL query", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

    task.cluster_name = task.query_on_cluster->cluster;
    task.cluster = context.tryGetCluster(task.cluster_name);
    if (!task.cluster)
        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
            "DDL task {} contains current host {} in cluster {}, but there are no such cluster here.",
            task.entry_name, task.host_id.readableString(), task.cluster_name);

    /// Try to find host from task host list in cluster
    /// At the first, try find exact match (host name and ports should be literally equal)
    /// If the attempt fails, try find it resolving host name of each instance
    const auto & shards = task.cluster->getShardsAddresses();

    bool found_exact_match = false;
    String default_database;
    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (address.host_name == task.host_id.host_name && address.port == task.host_id.port)
            {
                if (found_exact_match)
                {
                    if (default_database == address.default_database)
                    {
                        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                            "There are two exactly the same ClickHouse instances {} in cluster {}",
                            address.readableString(), task.cluster_name);
                    }
                    else
                    {
                        /* Circular replication is used.
                         * It is when every physical node contains
                         * replicas of different shards of the same table.
                         * To distinguish one replica from another on the same node,
                         * every shard is placed into separate database.
                         * */
                        is_circular_replicated = true;
                        auto * query_with_table = dynamic_cast<ASTQueryWithTableAndOutput *>(task.query.get());
                        if (!query_with_table || query_with_table->database.empty())
                        {
                            throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                "For a distributed DDL on circular replicated cluster its table name must be qualified by database name.");
                        }
                        if (default_database == query_with_table->database)
                            return;
                    }
                }
                found_exact_match = true;
                task.host_shard_num = shard_num;
                task.host_replica_num = replica_num;
                task.address_in_cluster = address;
                default_database = address.default_database;
            }
        }
    }

    if (found_exact_match)
        return;

    LOG_WARNING(log, "Not found the exact match of host {} from task {} in cluster {} definition. Will try to find it using host name resolving.", task.host_id.readableString(), task.entry_name, task.cluster_name);

    bool found_via_resolving = false;
    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (auto resolved = address.getResolvedAddress();
                resolved && (isLocalAddress(*resolved, context.getTCPPort())
                    || (context.getTCPPortSecure() && isLocalAddress(*resolved, *context.getTCPPortSecure()))))
            {
                if (found_via_resolving)
                {
                    throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                        "There are two the same ClickHouse instances in cluster {} : {} and {}",
                        task.cluster_name, task.address_in_cluster.readableString(), address.readableString());
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
        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
            "Not found host {} in definition of cluster {}",
            task.host_id.readableString(), task.cluster_name);
    }
    else
    {
        LOG_INFO(log, "Resolved host {} from task {} as host {} in definition of cluster {}", task.host_id.readableString(), task.entry_name, task.address_in_cluster.readableString(), task.cluster_name);
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
        auto current_context = std::make_unique<Context>(context);
        current_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        current_context->setCurrentQueryId(""); // generate random query_id
        executeQuery(istr, ostr, false, *current_context, {});
    }
    catch (...)
    {
        status = ExecutionStatus::fromCurrentException();
        tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");

        return false;
    }

    status = ExecutionStatus(0);
    LOG_DEBUG(log, "Executed query: {}", query);

    return true;
}

void DDLWorker::attachToThreadGroup()
{
    if (thread_group)
    {
        /// Put all threads to one thread pool
        CurrentThread::attachToIfDetached(thread_group);
    }
    else
    {
        CurrentThread::initializeQuery();
        thread_group = CurrentThread::getGroup();
    }
}


void DDLWorker::enqueueTask(DDLTaskPtr task_ptr)
{
    auto & task = *task_ptr;

    while (!stop_flag)
    {
        try
        {
            processTask(task);
            return;
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                recoverZooKeeper();
            }
            else if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_ERROR(log, "ZooKeeper error: {}", getCurrentExceptionMessage(true));
                // TODO: retry?
            }
            else
            {
                LOG_ERROR(log, "Unexpected ZooKeeper error: {}.", getCurrentExceptionMessage(true));
                return;
            }
        }
        catch (...)
        {
            LOG_WARNING(log, "An error occurred while processing task {} ({}) : {}", task.entry_name, task.entry.query, getCurrentExceptionMessage(true));
        }
    }
}
void DDLWorker::processTask(DDLTask & task)
{
    auto zookeeper = tryGetZooKeeper();

    LOG_DEBUG(log, "Processing task {} ({})", task.entry_name, task.entry.query);

    String dummy;
    String active_node_path = task.entry_path + "/active/" + task.host_id_str;
    String finished_node_path = task.entry_path + "/finished/" + task.host_id_str;

    auto code = zookeeper->tryCreate(active_node_path, "", zkutil::CreateMode::Ephemeral, dummy);

    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
    {
        // Ok
    }
    else if (code == Coordination::Error::ZNONODE)
    {
        /// There is no parent
        createStatusDirs(task.entry_path, zookeeper);
        if (Coordination::Error::ZOK != zookeeper->tryCreate(active_node_path, "", zkutil::CreateMode::Ephemeral, dummy))
            throw Coordination::Exception(code, active_node_path);
    }
    else
        throw Coordination::Exception(code, active_node_path);

    if (!task.was_executed)
    {
        try
        {
            is_circular_replicated = false;
            parseQueryAndResolveHost(task);

            ASTPtr rewritten_ast = task.query_on_cluster->getRewrittenASTWithoutOnCluster(task.address_in_cluster.default_database);
            String rewritten_query = queryToString(rewritten_ast);
            LOG_DEBUG(log, "Executing query: {}", rewritten_query);

            if (auto * query_with_table = dynamic_cast<ASTQueryWithTableAndOutput *>(rewritten_ast.get()); query_with_table)
            {
                StoragePtr storage;
                if (!query_with_table->table.empty())
                {
                    /// It's not CREATE DATABASE
                    auto table_id = context.tryResolveStorageID(*query_with_table, Context::ResolveOrdinary);
                    storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
                }

                if (storage && taskShouldBeExecutedOnLeader(rewritten_ast, storage)  && !is_circular_replicated)
                    tryExecuteQueryOnLeaderReplica(task, storage, rewritten_query, task.entry_path, zookeeper);
                else
                    tryExecuteQuery(rewritten_query, task, task.execution_status);
            }
            else
                tryExecuteQuery(rewritten_query, task, task.execution_status);
        }
        catch (const Coordination::Exception &)
        {
            throw;
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred before execution of DDL task: ");
            task.execution_status = ExecutionStatus::fromCurrentException("An error occurred before execution");
        }

        /// We need to distinguish ZK errors occurred before and after query executing
        task.was_executed = true;
    }

    /// FIXME: if server fails right here, the task will be executed twice. We need WAL here.

    /// Delete active flag and create finish flag
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(active_node_path, -1));
    ops.emplace_back(zkutil::makeCreateRequest(finished_node_path, task.execution_status.serializeText(), zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);
}


bool DDLWorker::taskShouldBeExecutedOnLeader(const ASTPtr ast_ddl, const StoragePtr storage)
{
    /// Pure DROP queries have to be executed on each node separately
    if (auto * query = ast_ddl->as<ASTDropQuery>(); query && query->kind != ASTDropQuery::Kind::Truncate)
        return false;

    if (!ast_ddl->as<ASTAlterQuery>() && !ast_ddl->as<ASTOptimizeQuery>() && !ast_ddl->as<ASTDropQuery>())
        return false;

    return storage->supportsReplication();
}

bool DDLWorker::tryExecuteQueryOnLeaderReplica(
    DDLTask & task,
    StoragePtr storage,
    const String & rewritten_query,
    const String & node_path,
    const ZooKeeperPtr & zookeeper)
{
    StorageReplicatedMergeTree * replicated_storage = dynamic_cast<StorageReplicatedMergeTree *>(storage.get());

    /// If we will develop new replicated storage
    if (!replicated_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Storage type '{}' is not supported by distributed DDL", storage->getName());

    /// Generate unique name for shard node, it will be used to execute the query by only single host
    /// Shard node name has format 'replica_name1,replica_name2,...,replica_nameN'
    /// Where replica_name is 'replica_config_host_name:replica_port'
    auto get_shard_name = [] (const Cluster::Addresses & shard_addresses)
    {
        Strings replica_names;
        for (const Cluster::Address & address : shard_addresses)
            replica_names.emplace_back(address.readableString());
        std::sort(replica_names.begin(), replica_names.end());

        String res;
        for (auto it = replica_names.begin(); it != replica_names.end(); ++it)
            res += *it + (std::next(it) != replica_names.end() ? "," : "");

        return res;
    };

    String shard_node_name = get_shard_name(task.cluster->getShardsAddresses().at(task.host_shard_num));
    String shard_path = node_path + "/shards/" + shard_node_name;
    String is_executed_path = shard_path + "/executed";
    String tries_to_execute_path = shard_path + "/tries_to_execute";
    zookeeper->createAncestors(shard_path + "/");

    /// Node exists, or we will create or we will get an exception
    zookeeper->tryCreate(tries_to_execute_path, "0", zkutil::CreateMode::Persistent);

    static constexpr int MAX_TRIES_TO_EXECUTE = 3;

    String executed_by;

    zkutil::EventPtr event = std::make_shared<Poco::Event>();
    if (zookeeper->tryGet(is_executed_path, executed_by, nullptr, event))
    {
        LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, executed_by);
        return true;
    }

    pcg64 rng(randomSeed());

    auto lock = createSimpleZooKeeperLock(zookeeper, shard_path, "lock", task.host_id_str);

    bool executed_by_leader = false;
    while (true)
    {
        StorageReplicatedMergeTree::Status status;
        replicated_storage->getStatus(status);

        /// Any replica which is leader tries to take lock
        if (status.is_leader && lock->tryLock())
        {
            /// In replicated merge tree we can have multiple leaders. So we can
            /// be "leader", but another "leader" replica may already execute
            /// this task.
            if (zookeeper->tryGet(is_executed_path, executed_by))
            {
                LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, executed_by);
                executed_by_leader = true;
                break;
            }

            /// Doing it exclusively
            size_t counter = parse<int>(zookeeper->get(tries_to_execute_path));
            if (counter > MAX_TRIES_TO_EXECUTE)
                break;

            zookeeper->set(tries_to_execute_path, toString(counter + 1));

            /// If the leader will unexpectedly changed this method will return false
            /// and on the next iteration new leader will take lock
            if (tryExecuteQuery(rewritten_query, task, task.execution_status))
            {
                zookeeper->create(is_executed_path, task.host_id_str, zkutil::CreateMode::Persistent);
                executed_by_leader = true;
                break;
            }

            lock->unlock();
        }


        if (event->tryWait(std::uniform_int_distribution<int>(0, 1000)(rng)))
        {
            LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, zookeeper->get(is_executed_path));
            executed_by_leader = true;
            break;
        }
        else if (parse<int>(zookeeper->get(tries_to_execute_path)) > MAX_TRIES_TO_EXECUTE)
        {
            /// Nobody will try to execute query again
            break;
        }
    }

    /// Not executed by leader so was not executed at all
    if (!executed_by_leader)
    {
        task.execution_status = ExecutionStatus(ErrorCodes::NOT_IMPLEMENTED, "Cannot execute replicated DDL query");
        return false;
    }

    LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, zookeeper->get(is_executed_path));
    return true;
}


void DDLWorker::cleanupQueue(Int64 current_time_seconds, const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Cleaning queue");

    Strings queue_nodes = zookeeper->getChildren(queue_dir);
    filterAndSortQueueNodes(queue_nodes);

    size_t num_outdated_nodes = (queue_nodes.size() > max_tasks_in_queue) ? queue_nodes.size() - max_tasks_in_queue : 0;
    auto first_non_outdated_node = queue_nodes.begin() + num_outdated_nodes;

    for (auto it = queue_nodes.cbegin(); it < queue_nodes.cend(); ++it)
    {
        if (stop_flag)
            return;

        String node_name = *it;
        String node_path = queue_dir + "/" + node_name;
        String lock_path = node_path + "/lock";

        Coordination::Stat stat;
        String dummy;

        try
        {
            /// Already deleted
            if (!zookeeper->exists(node_path, &stat))
                continue;

            /// Delete node if its lifetime is expired (according to task_max_lifetime parameter)
            constexpr UInt64 zookeeper_time_resolution = 1000;
            Int64 zookeeper_time_seconds = stat.ctime / zookeeper_time_resolution;
            bool node_lifetime_is_expired = zookeeper_time_seconds + task_max_lifetime < current_time_seconds;

            /// If too many nodes in task queue (> max_tasks_in_queue), delete oldest one
            bool node_is_outside_max_window = it < first_non_outdated_node;

            if (!node_lifetime_is_expired && !node_is_outside_max_window)
                continue;

            /// Skip if there are active nodes (it is weak guard)
            if (zookeeper->exists(node_path + "/active", &stat) && stat.numChildren > 0)
            {
                LOG_INFO(log, "Task {} should be deleted, but there are active workers. Skipping it.", node_name);
                continue;
            }

            /// Usage of the lock is not necessary now (tryRemoveRecursive correctly removes node in a presence of concurrent cleaners)
            /// But the lock will be required to implement system.distributed_ddl_queue table
            auto lock = createSimpleZooKeeperLock(zookeeper, node_path, "lock", host_fqdn_id);
            if (!lock->tryLock())
            {
                LOG_INFO(log, "Task {} should be deleted, but it is locked. Skipping it.", node_name);
                continue;
            }

            if (node_lifetime_is_expired)
                LOG_INFO(log, "Lifetime of task {} is expired, deleting it", node_name);
            else if (node_is_outside_max_window)
                LOG_INFO(log, "Task {} is outdated, deleting it", node_name);

            /// Deleting
            {
                Strings children = zookeeper->getChildren(node_path);
                for (const String & child : children)
                {
                    if (child != "lock")
                        zookeeper->tryRemoveRecursive(node_path + "/" + child);
                }

                /// Remove the lock node and its parent atomically
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeRemoveRequest(lock_path, -1));
                ops.emplace_back(zkutil::makeRemoveRequest(node_path, -1));
                zookeeper->multi(ops);
            }
        }
        catch (...)
        {
            LOG_INFO(log, "An error occurred while checking and cleaning task {} from queue: {}", node_name, getCurrentExceptionMessage(false));
        }
    }
}


/// Try to create nonexisting "status" dirs for a node
void DDLWorker::createStatusDirs(const std::string & node_path, const ZooKeeperPtr & zookeeper)
{
    Coordination::Requests ops;
    {
        Coordination::CreateRequest request;
        request.path = node_path + "/active";
        ops.emplace_back(std::make_shared<Coordination::CreateRequest>(std::move(request)));
    }
    {
        Coordination::CreateRequest request;
        request.path = node_path + "/finished";
        ops.emplace_back(std::make_shared<Coordination::CreateRequest>(std::move(request)));
    }
    Coordination::Responses responses;
    Coordination::Error code = zookeeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK
        && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception(code);
}


String DDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    if (entry.hosts.empty())
        throw Exception("Empty host list in a distributed DDL task", ErrorCodes::LOGICAL_ERROR);

    auto zookeeper = getAndSetZooKeeper();

    String query_path_prefix = queue_dir + "/query-";
    zookeeper->createAncestors(query_path_prefix);

    String node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);

    /// Optional step
    try
    {
        createStatusDirs(node_path, zookeeper);
    }
    catch (...)
    {
        LOG_INFO(log, "An error occurred while creating auxiliary ZooKeeper directories in {} . They will be created later. Error : {}", node_path, getCurrentExceptionMessage(true));
    }

    return node_path;
}


void DDLWorker::runMainThread()
{
    setThreadName("DDLWorker");
    LOG_DEBUG(log, "Started DDLWorker thread");

    bool initialized = false;
    do
    {
        try
        {
            auto zookeeper = getAndSetZooKeeper();
            zookeeper->createAncestors(queue_dir + "/");
            initialized = true;
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
                throw;  /// A logical error.

            tryLogCurrentException(__PRETTY_FUNCTION__);

            /// Avoid busy loop when ZooKeeper is not available.
            sleepForSeconds(1);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Terminating. Cannot initialize DDL queue.");
            return;
        }
    }
    while (!initialized && !stop_flag);

    while (!stop_flag)
    {
        try
        {
            attachToThreadGroup();

            cleanup_event->set();
            scheduleTasks();

            LOG_DEBUG(log, "Waiting a watch");
            queue_updated_event->wait();
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                recoverZooKeeper();
            }
            else if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_ERROR(log, "ZooKeeper error: {}", getCurrentExceptionMessage(true));
            }
            else
            {
                LOG_ERROR(log, "Unexpected ZooKeeper error: {}. Terminating.", getCurrentExceptionMessage(true));
                return;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Unexpected error, will terminate:");
            return;
        }
    }
}


void DDLWorker::runCleanupThread()
{
    setThreadName("DDLWorkerClnr");
    LOG_DEBUG(log, "Started DDLWorker cleanup thread");

    Int64 last_cleanup_time_seconds = 0;
    while (!stop_flag)
    {
        try
        {
            cleanup_event->wait();
            if (stop_flag)
                break;

            Int64 current_time_seconds = Poco::Timestamp().epochTime();
            if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_delay_period)
            {
                LOG_TRACE(log, "Too early to clean queue, will do it later.");
                continue;
            }

            auto zookeeper = tryGetZooKeeper();
            if (zookeeper->expired())
                continue;

            cleanupQueue(current_time_seconds, zookeeper);
            last_cleanup_time_seconds = current_time_seconds;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}


class DDLQueryStatusInputStream : public IBlockInputStream
{
public:

    DDLQueryStatusInputStream(const String & zk_node_path, const DDLLogEntry & entry, const Context & context_)
        : node_path(zk_node_path), context(context_), watch(CLOCK_MONOTONIC_COARSE), log(&Poco::Logger::get("DDLQueryStatusInputStream"))
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

        addTotalRowsApprox(entry.hosts.size());

        timeout_seconds = context.getSettingsRef().distributed_ddl_task_timeout;
    }

    String getName() const override
    {
        return "DDLQueryStatusInputStream";
    }

    Block getHeader() const override { return sample; }

    Block readImpl() override
    {
        Block res;
        if (num_hosts_finished >= waiting_hosts.size())
        {
            if (first_exception)
                throw Exception(*first_exception);

            return res;
        }

        auto zookeeper = context.getZooKeeper();
        size_t try_number = 0;

        while (res.rows() == 0)
        {
            if (isCancelled())
            {
                if (first_exception)
                    throw Exception(*first_exception);

                return res;
            }

            if (timeout_seconds >= 0 && watch.elapsedSeconds() > timeout_seconds)
            {
                size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
                size_t num_active_hosts = current_active_hosts.size();


                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                    "Watching task {} is executing longer than distributed_ddl_task_timeout (={}) seconds. "
                    "There are {} unfinished hosts ({} of them are currently active), they are going to execute the query in background",
                    node_path, timeout_seconds, num_unfinished_hosts, num_active_hosts);
            }

            if (num_hosts_finished != 0 || try_number != 0)
            {
                sleepForMilliseconds(std::min<size_t>(1000, 50 * (try_number + 1)));
            }

            /// TODO: add shared lock
            if (!zookeeper->exists(node_path))
            {
                throw Exception(ErrorCodes::UNFINISHED,
                    "Cannot provide query execution status. The query's node {} has been deleted by the cleaner since it was finished (or its lifetime is expired)",
                    node_path);
            }

            Strings new_hosts = getNewAndUpdate(getChildrenAllowNoNode(zookeeper, node_path + "/finished"));
            ++try_number;
            if (new_hosts.empty())
                continue;

            current_active_hosts = getChildrenAllowNoNode(zookeeper, node_path + "/active");

            MutableColumns columns = sample.cloneEmptyColumns();
            for (const String & host_id : new_hosts)
            {
                ExecutionStatus status(-1, "Cannot obtain error message");
                {
                    String status_data;
                    if (zookeeper->tryGet(node_path + "/finished/" + host_id, status_data))
                        status.tryDeserializeText(status_data);
                }

                auto [host, port] = Cluster::Address::fromString(host_id);

                if (status.code != 0 && first_exception == nullptr)
                    first_exception = std::make_unique<Exception>(status.code, "There was an error on [{}:{}]: {}", host, port, status.message);

                ++num_hosts_finished;

                columns[0]->insert(host);
                columns[1]->insert(port);
                columns[2]->insert(status.code);
                columns[3]->insert(status.message);
                columns[4]->insert(waiting_hosts.size() - num_hosts_finished);
                columns[5]->insert(current_active_hosts.size());
            }
            res = sample.cloneWithColumns(std::move(columns));
        }

        return res;
    }

    Block getSampleBlock() const
    {
        return sample.cloneEmpty();
    }

    ~DDLQueryStatusInputStream() override = default;

private:

    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
    {
        Strings res;
        Coordination::Error code = zookeeper->tryGetChildren(node_path, res);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw Coordination::Exception(code, node_path);
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
                    LOG_INFO(log, "Unexpected host {} appeared  in task {}", host, node_path);
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

    String node_path;
    const Context & context;
    Stopwatch watch;
    Poco::Logger * log;

    Block sample;

    NameSet waiting_hosts;  /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that were in active state at the last check
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
};


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, const Context & context, AccessRightsElements && query_requires_access, bool query_requires_grant_option)
{
    /// Remove FORMAT <fmt> and INTO OUTFILE <file> if exists
    ASTPtr query_ptr = query_ptr_->clone();
    ASTQueryWithOutput::resetOutputASTIfExist(*query_ptr);

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    auto * query = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Distributed execution is not supported for such DDL queries", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!context.getSettingsRef().allow_distributed_ddl)
        throw Exception("Distributed DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    if (const auto * query_alter = query_ptr->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->commands)
        {
            if (!isSupportedAlterType(command->type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    query->cluster = context.getMacros()->expand(query->cluster);
    ClusterPtr cluster = context.getCluster(query->cluster);
    DDLWorker & ddl_worker = context.getDDLWorker();

    /// Enumerate hosts which will be used to send query.
    Cluster::AddressesWithFailover shards = cluster->getShardsAddresses();
    std::vector<HostID> hosts;
    for (const auto & shard : shards)
    {
        for (const auto & addr : shard)
            hosts.emplace_back(addr);
    }

    if (hosts.empty())
        throw Exception("No hosts defined to execute distributed DDL query", ErrorCodes::LOGICAL_ERROR);

    /// The current database in a distributed query need to be replaced with either
    /// the local current database or a shard's default database.
    bool need_replace_current_database
        = (std::find_if(
               query_requires_access.begin(),
               query_requires_access.end(),
               [](const AccessRightsElement & elem) { return elem.isEmptyDatabase(); })
           != query_requires_access.end());

    bool use_local_default_database = false;
    const String & current_database = context.getCurrentDatabase();

    if (need_replace_current_database)
    {
        Strings shard_default_databases;
        for (const auto & shard : shards)
        {
            for (const auto & addr : shard)
            {
                if (!addr.default_database.empty())
                    shard_default_databases.push_back(addr.default_database);
                else
                    use_local_default_database = true;
            }
        }
        std::sort(shard_default_databases.begin(), shard_default_databases.end());
        shard_default_databases.erase(std::unique(shard_default_databases.begin(), shard_default_databases.end()), shard_default_databases.end());
        assert(use_local_default_database || !shard_default_databases.empty());

        if (use_local_default_database && !shard_default_databases.empty())
            throw Exception("Mixed local default DB and shard default DB in DDL query", ErrorCodes::NOT_IMPLEMENTED);

        if (use_local_default_database)
        {
            query_requires_access.replaceEmptyDatabase(current_database);
        }
        else
        {
            for (size_t i = 0; i != query_requires_access.size();)
            {
                auto & element = query_requires_access[i];
                if (element.isEmptyDatabase())
                {
                    query_requires_access.insert(query_requires_access.begin() + i + 1, shard_default_databases.size() - 1, element);
                    for (size_t j = 0; j != shard_default_databases.size(); ++j)
                        query_requires_access[i + j].replaceEmptyDatabase(shard_default_databases[j]);
                    i += shard_default_databases.size();
                }
                else
                    ++i;
            }
        }
    }

    AddDefaultDatabaseVisitor visitor(current_database, !use_local_default_database);
    visitor.visitDDL(query_ptr);

    /// Check access rights, assume that all servers have the same users config
    if (query_requires_grant_option)
        context.getAccess()->checkGrantOption(query_requires_access);
    else
        context.checkAccess(query_requires_access);

    DDLLogEntry entry;
    entry.hosts = std::move(hosts);
    entry.query = queryToString(query_ptr);
    entry.initiator = ddl_worker.getCommonHostID();
    String node_path = ddl_worker.enqueueQuery(entry);

    BlockIO io;
    if (context.getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    auto stream = std::make_shared<DDLQueryStatusInputStream>(node_path, entry, context);
    io.in = std::move(stream);
    return io;
}

BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, const Context & context, const AccessRightsElements & query_requires_access, bool query_requires_grant_option)
{
    return executeDDLQueryOnCluster(query_ptr, context, AccessRightsElements{query_requires_access}, query_requires_grant_option);
}

BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, const Context & context)
{
    return executeDDLQueryOnCluster(query_ptr_, context, {});
}

}
