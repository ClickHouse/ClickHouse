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
    extern const int DATABASE_REPLICATION_FAILED;
    extern const int UNKNOWN_DATABASE;
}

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

    replica_path = zookeeper_path + "/replicas/" + shard_name + "/" + replica_name;

    String replica_host_id;
    if (current_zookeeper->tryGet(replica_path, replica_host_id))
    {
        String host_id = getHostID(global_context);
        if (replica_host_id != host_id)
            throw Exception(ErrorCodes::REPLICA_IS_ALREADY_EXIST,
                            "Replica {} of shard {} of replicated database at {} already exists. Replica host ID: '{}', current host ID: '{}'",
                            replica_name, shard_name, zookeeper_path, replica_host_id, host_id);

        log_entry_to_execute = parse<UInt32>(current_zookeeper->get(replica_path + "/log_ptr"));
    }
    else
    {
        /// Throws if replica with the same name was created concurrently
        createReplicaNodesInZooKeeper(current_zookeeper);
    }

    snapshot_period = 1; //context_.getConfigRef().getInt("database_replicated_snapshot_period", 10);
    LOG_DEBUG(log, "Snapshot period is set to {} log entries per one snapshot", snapshot_period);
}

bool DatabaseReplicated::createDatabaseNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    current_zookeeper->createAncestors(zookeeper_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/min_log_ptr", "0", zkutil::CreateMode::Persistent));

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

    /// When creating new replica, use latest snapshot version as initial value of log_pointer
    log_entry_to_execute = 0;   //FIXME

    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    auto host_id = getHostID(global_context);

    /// On replica creation add empty entry to log. Can be used to trigger some actions on other replicas (e.g. update cluster info).
    DDLLogEntry entry;
    entry.hosts = {};
    entry.query = {};
    entry.initiator = {};

    String query_path_prefix = zookeeper_path + "/log/query-";
    String counter_prefix = zookeeper_path + "/counter/cnt-";
    String counter_path = current_zookeeper->create(counter_prefix, "", zkutil::CreateMode::EphemeralSequential);
    String query_path = query_path_prefix + counter_path.substr(counter_prefix.size());

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_ptr", toString(log_entry_to_execute), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(query_path, entry.toString(), zkutil::CreateMode::PersistentSequential));
    ops.emplace_back(zkutil::makeRemoveRequest(counter_path, -1));
    current_zookeeper->multi(ops);
}

void DatabaseReplicated::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    DatabaseAtomic::loadStoredObjects(context, has_force_restore_data_flag, force_attach);

    recoverLostReplica(global_context.getZooKeeper(), 0, true); //FIXME

    DatabaseReplicatedExtensions ext;
    ext.database_uuid = getUUID();
    ext.zookeeper_path = zookeeper_path;
    ext.database_name = getDatabaseName();
    ext.shard_name = shard_name;
    ext.replica_name = replica_name;
    ext.first_not_executed = log_entry_to_execute;
    ext.lost_callback     = [this] (const String & entry_name, const ZooKeeperPtr & zookeeper) { onUnexpectedLogEntry(entry_name, zookeeper); };
    ext.executed_callback = [this] (const String & entry_name, const ZooKeeperPtr & zookeeper) { onExecutedLogEntry(entry_name, zookeeper); };

    /// Pool size must be 1 (to avoid reordering of log entries)
    constexpr size_t pool_size = 1;
    ddl_worker = std::make_unique<DDLWorker>(pool_size, zookeeper_path + "/log", global_context, nullptr, "",
                                             std::make_optional<DatabaseReplicatedExtensions>(std::move(ext)));
}

void DatabaseReplicated::onUnexpectedLogEntry(const String & entry_name, const ZooKeeperPtr & zookeeper)
{
    /// We cannot execute next entry of replication log. Possible reasons:
    /// 1. Replica is staled, some entries were removed by log cleanup process.
    ///    In this case we should recover replica from the last snapshot.
    /// 2. Replication log is broken due to manual operations with ZooKeeper or logical error.
    ///    In this case we just stop replication without any attempts to recover it automatically,
    ///    because such attempts may lead to unexpected data removal.

    constexpr const char * name = "query-";
    if (!startsWith(entry_name, name))
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Unexpected entry in replication log: {}", entry_name);

    UInt32 entry_number;
    if (!tryParse(entry_number, entry_name.substr(strlen(name))))
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Cannot parse number of replication log entry {}", entry_name);

    if (entry_number < log_entry_to_execute)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Entry {} already executed, current pointer is {}", entry_number, log_entry_to_execute);

    /// Entry name is valid. Let's get min log pointer to check if replica is staled.
    UInt32 min_snapshot = parse<UInt32>(zookeeper->get(zookeeper_path + "/min_log_ptr"));

    if (log_entry_to_execute < min_snapshot)
    {
        recoverLostReplica(zookeeper, 0);   //FIXME log_pointer
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot recover replica, probably it's a bug. "
                                               "Got log entry '{}' when expected entry number {}");
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

void DatabaseReplicated::onExecutedLogEntry(const String & /*entry_name*/, const ZooKeeperPtr & /*zookeeper*/)
{

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
    hosts_to_wait.emplace_back(shard_name + '|' +replica_name);
    auto stream = std::make_shared<DDLQueryStatusInputStream>(node_path, entry, global_context);
    io.in = std::move(stream);
    return io;
}


void DatabaseReplicated::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 from_snapshot, bool create)
{
    LOG_WARNING(log, "Will recover replica from snapshot", from_snapshot);

    //FIXME drop old tables

    String snapshot_metadata_path = zookeeper_path + "/metadata";
    Strings tables_in_snapshot = current_zookeeper->getChildren(snapshot_metadata_path);
    snapshot_metadata_path += '/';

    for (const auto & table_name : tables_in_snapshot)
    {
        //FIXME It's not atomic. We need multiget here (available since ZooKeeper 3.6.0).
        String query_to_execute = current_zookeeper->get(snapshot_metadata_path + table_name);


        if (!startsWith(query_to_execute, "ATTACH "))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected query: {}", query_to_execute);
        query_to_execute = "CREATE " + query_to_execute.substr(strlen("ATTACH "));

        Context current_context = global_context;
        current_context.getClientInfo().query_kind = ClientInfo::QueryKind::REPLICATED_LOG_QUERY;
        current_context.setCurrentDatabase(database_name);
        current_context.setCurrentQueryId(""); // generate random query_id

        executeQuery(query_to_execute, current_context);
    }

    if (create)
        return;

    current_zookeeper->set(replica_path + "/log-ptr", toString(from_snapshot));
    last_executed_log_entry = from_snapshot;
    ddl_worker->setLogPointer(from_snapshot); //FIXME

    //writeLastExecutedToDiskAndZK();
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
