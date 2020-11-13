#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/Cluster.h>
#include <common/getFQDNOrHostName.h>
#include <Parsers/ASTAlterQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int REPLICA_IS_ALREADY_EXIST;
}

constexpr const char * first_entry_name = "query-0000000000";

zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    return global_context.getZooKeeper();
}

static inline String getHostID(const Context & global_context)
{
    return Cluster::Address::toString(getFQDNOrHostName(), global_context.getTCPPort());
}


DatabaseReplicated::~DatabaseReplicated() = default;

DatabaseReplicated::DatabaseReplicated(
    const String & name_,
    const String & metadata_path_,
    UUID uuid,
    const String & zookeeper_path_,
    const String & shard_name_,
    const String & replica_name_,
    Context & context_)
    : DatabaseAtomic(name_, metadata_path_, uuid, "DatabaseReplicated (" + name_ + ")", context_)
    , zookeeper_path(zookeeper_path_)
    , shard_name(shard_name_)
    , replica_name(replica_name_)
{
    if (zookeeper_path.empty() || shard_name.empty() || replica_name.empty())
        throw Exception("ZooKeeper path, shard and replica names must be non-empty", ErrorCodes::BAD_ARGUMENTS);
    if (shard_name.find('/') != std::string::npos || replica_name.find('/') != std::string::npos)
        throw Exception("Shard and replica names should not contain '/'", ErrorCodes::BAD_ARGUMENTS);

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    if (!context_.hasZooKeeper())
    {
        throw Exception("Can't create replicated database without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    }
    //FIXME it will fail on startup if zk is not available

    auto current_zookeeper = global_context.getZooKeeper();

    if (!current_zookeeper->exists(zookeeper_path))
    {
        /// Create new database, multiple nodes can execute it concurrently
        createDatabaseNodesInZooKeeper(current_zookeeper);
    }

    replica_path = zookeeper_path + "/replicas/" + shard_name + "|" + replica_name;

    String replica_host_id;
    if (current_zookeeper->tryGet(replica_path, replica_host_id))
    {
        String host_id = getHostID(global_context);
        if (replica_host_id != host_id)
            throw Exception(ErrorCodes::REPLICA_IS_ALREADY_EXIST,
                            "Replica {} of shard {} of replicated database at {} already exists. Replica host ID: '{}', current host ID: '{}'",
                            replica_name, shard_name, zookeeper_path, replica_host_id, host_id);

        log_entry_to_execute = current_zookeeper->get(replica_path + "/log_ptr");
    }
    else
    {
        /// Throws if replica with the same name was created concurrently
        createReplicaNodesInZooKeeper(current_zookeeper);
    }

    assert(log_entry_to_execute.starts_with("query-"));


    snapshot_period = context_.getConfigRef().getInt("database_replicated_snapshot_period", 10);
    LOG_DEBUG(log, "Snapshot period is set to {} log entries per one snapshot", snapshot_period);
}

bool DatabaseReplicated::createDatabaseNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    current_zookeeper->createAncestors(zookeeper_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/snapshots", "", zkutil::CreateMode::Persistent));
    /// Create empty snapshot (with no tables)
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/snapshots/" + first_entry_name, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto res = current_zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZOK)
        return true;
    if (res == Coordination::Error::ZNODEEXISTS)
        return false;

    zkutil::KeeperMultiException::check(res, ops, responses);
    assert(false);
}

void DatabaseReplicated::createReplicaNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    current_zookeeper->createAncestors(replica_path);

    Strings snapshots = current_zookeeper->getChildren(zookeeper_path + "/snapshots");
    std::sort(snapshots.begin(), snapshots.end());
    if (snapshots.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No snapshots found");

    /// When creating new replica, use latest snapshot version as initial value of log_pointer
    log_entry_to_execute = snapshots.back();

    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    auto host_id = getHostID(global_context);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_ptr", log_entry_to_execute , zkutil::CreateMode::Persistent));
    current_zookeeper->multi(ops);
}

void DatabaseReplicated::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    DatabaseAtomic::loadStoredObjects(context, has_force_restore_data_flag, force_attach);

    DatabaseReplicatedExtensions ext;
    ext.database_uuid = getUUID();
    ext.database_name = getDatabaseName();
    ext.shard_name = shard_name;
    ext.replica_name = replica_name;
    ext.first_not_executed = log_entry_to_execute;

    /// Pool size must be 1 (to avoid reordering of log entries)
    constexpr size_t pool_size = 1;
    ddl_worker = std::make_unique<DDLWorker>(pool_size, zookeeper_path + "/log", global_context, nullptr, "",
                                             std::make_optional<DatabaseReplicatedExtensions>(std::move(ext)));
}


void DatabaseReplicated::removeOutdatedSnapshotsAndLog()
{
    /// This method removes all snapshots and logged queries
    /// that no longer will be in use by current replicas or
    /// new coming ones.
    /// Each registered replica has its state in ZooKeeper.
    /// Therefore, snapshots and logged queries that are less
    /// than a least advanced replica are removed.
    /// It does not interfere with a new coming replica
    /// metadata loading from snapshot
    /// because the replica will use the latest snapshot available
    /// and this snapshot will set the last executed log query
    /// to a greater one than the least advanced current replica.
    auto current_zookeeper = getZooKeeper();
    Strings replica_states = current_zookeeper->getChildren(zookeeper_path + "/replicas");
    //TODO do not use log pointers to determine which entries to remove if there are staled pointers.
    // We can just remove all entries older than previous snapshot version.
    // Possible invariant: store all entries since last snapshot, replica becomes lost when it cannot get log entry.
    auto least_advanced = std::min_element(replica_states.begin(), replica_states.end());
    Strings snapshots = current_zookeeper->getChildren(zookeeper_path + "/snapshots");

    if (snapshots.size() < 2)
    {
        return;
    }

    std::sort(snapshots.begin(), snapshots.end());
    auto still_useful = std::lower_bound(snapshots.begin(), snapshots.end(), *least_advanced);
    snapshots.erase(still_useful, snapshots.end());
    for (const String & snapshot : snapshots)
    {
        current_zookeeper->tryRemoveRecursive(zookeeper_path + "/snapshots/" + snapshot);
    }

    Strings log_entry_names = current_zookeeper->getChildren(zookeeper_path + "/log");
    std::sort(log_entry_names.begin(), log_entry_names.end());
    auto still_useful_log = std::upper_bound(log_entry_names.begin(), log_entry_names.end(), *still_useful);
    log_entry_names.erase(still_useful_log, log_entry_names.end());
    for (const String & log_entry_name : log_entry_names)
    {
        String log_entry_path = zookeeper_path + "/log/" + log_entry_name;
        current_zookeeper->tryRemove(log_entry_path);
    }
}

void DatabaseReplicated::runBackgroundLogExecutor()
{
    if (last_executed_log_entry.empty())
    {
        loadMetadataFromSnapshot();
    }

    auto current_zookeeper = getZooKeeper();
    Strings log_entry_names = current_zookeeper->getChildren(zookeeper_path + "/log");

    std::sort(log_entry_names.begin(), log_entry_names.end());
    auto newest_entry_it = std::upper_bound(log_entry_names.begin(), log_entry_names.end(), last_executed_log_entry);

    log_entry_names.erase(log_entry_names.begin(), newest_entry_it);

    for (const String & log_entry_name : log_entry_names)
    {
        //executeLogName(log_entry_name);
        last_executed_log_entry = log_entry_name;
        writeLastExecutedToDiskAndZK();

        int log_n = parse<int>(log_entry_name.substr(4));
        int last_log_n = parse<int>(log_entry_names.back().substr(4));

        /// The third condition gurantees at most one snapshot creation per batch
        if (log_n > 0 && snapshot_period > 0 && (last_log_n - log_n) / snapshot_period == 0 && log_n % snapshot_period == 0)
        {
            createSnapshot();
        }
    }

    //background_log_executor->scheduleAfter(500);
}

void DatabaseReplicated::writeLastExecutedToDiskAndZK()
{
    auto current_zookeeper = getZooKeeper();
    current_zookeeper->createOrUpdate(
        zookeeper_path + "/replicas/" + replica_name, last_executed_log_entry, zkutil::CreateMode::Persistent);

    String metadata_file = getMetadataPath() + ".last_entry";
    WriteBufferFromFile out(metadata_file, last_executed_log_entry.size(), O_WRONLY | O_CREAT);
    writeString(last_executed_log_entry, out);
    out.next();
    if (global_context.getSettingsRef().fsync_metadata)
        out.sync();
    out.close();
}


BlockIO DatabaseReplicated::propose(const ASTPtr & query)
{
    if (const auto * query_alter = query->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->commands)
        {
            //FIXME allow all types of queries (maybe we should execute ATTACH an similar queries on leader)
            if (!isSupportedAlterType(command->type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    LOG_DEBUG(log, "Proposing query: {}", queryToString(query));

    DDLLogEntry entry;
    entry.hosts = {};
    entry.query = queryToString(query);
    entry.initiator = ddl_worker->getCommonHostID();
    String node_path = ddl_worker->enqueueQuery(entry);

    BlockIO io;
    //FIXME use query context
    if (global_context.getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    //FIXME need list of all replicas, we can obtain it from zk
    Strings hosts_to_wait;
    hosts_to_wait.emplace_back(shard_name + '/' +replica_name);
    auto stream = std::make_shared<DDLQueryStatusInputStream>(node_path, entry, global_context);
    io.in = std::move(stream);
    return io;
}


void DatabaseReplicated::createSnapshot()
{
    auto current_zookeeper = getZooKeeper();
    String snapshot_path = zookeeper_path + "/snapshots/" + last_executed_log_entry;

    if (Coordination::Error::ZNODEEXISTS == current_zookeeper->tryCreate(snapshot_path, String(), zkutil::CreateMode::Persistent))
    {
        return;
    }

    for (auto iterator = getTablesIterator(global_context, {}); iterator->isValid(); iterator->next())
    {
        String table_name = iterator->name();
        auto query = getCreateQueryFromMetadata(getObjectMetadataPath(table_name), true);
        String statement = queryToString(query);
        current_zookeeper->create(snapshot_path + "/" + table_name, statement, zkutil::CreateMode::Persistent);
    }
    current_zookeeper->create(snapshot_path + "/.completed", String(), zkutil::CreateMode::Persistent);

    removeOutdatedSnapshotsAndLog();
}

void DatabaseReplicated::loadMetadataFromSnapshot()
{
    /// Executes the latest snapshot.
    /// Used by new replicas only.
    auto current_zookeeper = getZooKeeper();

    Strings snapshots;
    if (current_zookeeper->tryGetChildren(zookeeper_path + "/snapshots", snapshots) != Coordination::Error::ZOK)
        return;

    auto latest_snapshot = std::max_element(snapshots.begin(), snapshots.end());
    while (snapshots.size() > 0 && !current_zookeeper->exists(zookeeper_path + "/snapshots/" + *latest_snapshot + "/.completed"))
    {
        snapshots.erase(latest_snapshot);
        latest_snapshot = std::max_element(snapshots.begin(), snapshots.end());
    }

    if (snapshots.size() < 1)
    {
        return;
    }

    Strings metadatas;
    if (current_zookeeper->tryGetChildren(zookeeper_path + "/snapshots/" + *latest_snapshot, metadatas) != Coordination::Error::ZOK)
        return;

    LOG_DEBUG(log, "Executing {} snapshot", *latest_snapshot);

    for (auto t = metadatas.begin(); t != metadatas.end(); ++t)
    {
        String path = zookeeper_path + "/snapshots/" + *latest_snapshot + "/" + *t;

        String query_to_execute = current_zookeeper->get(path, {}, nullptr);

        auto current_context = std::make_unique<Context>(global_context);
        current_context->getClientInfo().query_kind = ClientInfo::QueryKind::REPLICATED_LOG_QUERY;
        current_context->setCurrentDatabase(database_name);
        current_context->setCurrentQueryId(""); // generate random query_id

        executeQuery(query_to_execute, *current_context);
    }

    last_executed_log_entry = *latest_snapshot;
    writeLastExecutedToDiskAndZK();
}

void DatabaseReplicated::drop(const Context & context_)
{
    auto current_zookeeper = getZooKeeper();
    current_zookeeper->tryRemove(zookeeper_path + "/replicas/" + replica_name);
    DatabaseAtomic::drop(context_);
}

void DatabaseReplicated::shutdown()
{
    if (ddl_worker)
    {
        ddl_worker->shutdown();
        ddl_worker = nullptr;
    }
    DatabaseAtomic::shutdown();
}

}
