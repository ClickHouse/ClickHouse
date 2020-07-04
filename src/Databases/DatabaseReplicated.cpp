#include <Databases/DatabaseReplicated.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Lock.h>
#include <DataTypes/DataTypeString.h>

#include <common/sleep.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

void DatabaseReplicated::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard lock(current_zookeeper_mutex);
    current_zookeeper = zookeeper;
}

zkutil::ZooKeeperPtr DatabaseReplicated::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}


DatabaseReplicated::DatabaseReplicated(
    const String & name_,
    const String & metadata_path_,
    const String & zookeeper_path_,
    const String & replica_name_,
    Context & context_)
    : DatabaseAtomic(name_, metadata_path_, "store/", "DatabaseReplicated (" + name_ + ")", context_)
    , zookeeper_path(zookeeper_path_)
    , replica_name(replica_name_)
{
    if (zookeeper_path.empty() || replica_name.empty()) {
        throw Exception("ZooKeeper path and replica name must be non-empty", ErrorCodes::BAD_ARGUMENTS);
    }

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    // If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    if (context_.hasZooKeeper()) {
        current_zookeeper = context_.getZooKeeper();
    }
    if (!current_zookeeper)
    {
            throw Exception("Can't create replicated database without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    }

    // New database
    if (!current_zookeeper->exists(zookeeper_path)) {
        createDatabaseZKNodes();
    // Old replica recovery
    } else if (current_zookeeper->exists(zookeeper_path + "/replicas/" + replica_name)) {
        String remote_last_entry = current_zookeeper->get(zookeeper_path + "/replicas/" + replica_name, {}, NULL);

        String local_last_entry;
        try
        {
            ReadBufferFromFile in(getMetadataPath() + ".last_entry", 16);
            readStringUntilEOF(local_last_entry, in);
        }
        catch (const Exception & e)
        {
                // Metadata is corrupted.
                // Replica erases the previous zk last executed log entry
                // and behaves like a new clean replica.
                writeLastExecutedToDiskAndZK();
        }

        if (!local_last_entry.empty() && local_last_entry == remote_last_entry) {
            last_executed_log_entry = local_last_entry;
        } else {
            throw Exception("Replica name might be in use by a different node. Please check replica_name parameter. Remove .last_entry file from metadata to create a new replica.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    snapshot_period = context_.getConfigRef().getInt("database_replicated_snapshot_period", 10);
    feedback_timeout = context_.getConfigRef().getInt("database_replicated_feedback_timeout", 0);
    LOG_DEBUG(log, "Snapshot period is set to {} log entries per one snapshot", snapshot_period);

    background_log_executor = context_.getReplicatedSchedulePool().createTask(database_name + "(DatabaseReplicated::background_executor)", [this]{ runBackgroundLogExecutor();} );

    background_log_executor->scheduleAfter(500);
}

DatabaseReplicated::~DatabaseReplicated() = default;

void DatabaseReplicated::createDatabaseZKNodes() {
    current_zookeeper = getZooKeeper();

    current_zookeeper->createAncestors(zookeeper_path);

    current_zookeeper->createIfNotExists(zookeeper_path, String());
    current_zookeeper->createIfNotExists(zookeeper_path + "/log", String());
    current_zookeeper->createIfNotExists(zookeeper_path + "/snapshots", String());
    current_zookeeper->createIfNotExists(zookeeper_path + "/replicas", String());
}

void DatabaseReplicated::RemoveOutdatedSnapshotsAndLog() {
    // This method removes all snapshots and logged queries
    // that no longer will be in use by current replicas or
    // new coming ones.
    // Each registered replica has its state in ZooKeeper.
    // Therefore, snapshots and logged queries that are less
    // than a least advanced replica are removed.
    // It does not interfere with a new coming replica
    // metadata loading from snapshot
    // because the replica will use the latest snapshot available
    // and this snapshot will set the last executed log query
    // to a greater one than the least advanced current replica.
    current_zookeeper = getZooKeeper();
    Strings replica_states = current_zookeeper->getChildren(zookeeper_path + "/replicas");
    auto least_advanced = std::min_element(replica_states.begin(), replica_states.end());
    Strings snapshots = current_zookeeper->getChildren(zookeeper_path + "/snapshots");
    
    if (snapshots.size() < 2) {
        return;
    }

    std::sort(snapshots.begin(), snapshots.end());
    auto still_useful = std::lower_bound(snapshots.begin(), snapshots.end(), *least_advanced);
    snapshots.erase(still_useful, snapshots.end());
    for (const String & snapshot : snapshots) {
        current_zookeeper->tryRemoveRecursive(zookeeper_path + "/snapshots/" + snapshot);
    }

    Strings log_entry_names = current_zookeeper->getChildren(zookeeper_path + "/log");
    std::sort(log_entry_names.begin(), log_entry_names.end());
    auto still_useful_log = std::upper_bound(log_entry_names.begin(), log_entry_names.end(), *still_useful);
    log_entry_names.erase(still_useful_log, log_entry_names.end());
    for (const String & log_entry_name : log_entry_names) {
        String log_entry_path = zookeeper_path + "/log/" + log_entry_name;
        current_zookeeper->tryRemove(log_entry_path);
    }
}

void DatabaseReplicated::runBackgroundLogExecutor() {
    if (last_executed_log_entry == "") {
        loadMetadataFromSnapshot();
    }

    current_zookeeper = getZooKeeper();
    Strings log_entry_names = current_zookeeper->getChildren(zookeeper_path + "/log");

    std::sort(log_entry_names.begin(), log_entry_names.end());
    auto newest_entry_it = std::upper_bound(log_entry_names.begin(), log_entry_names.end(), last_executed_log_entry);

    log_entry_names.erase(log_entry_names.begin(), newest_entry_it);

    for (const String & log_entry_name : log_entry_names) {
        executeLogName(log_entry_name);
        last_executed_log_entry = log_entry_name;
        writeLastExecutedToDiskAndZK();

        int log_n = parse<int>(log_entry_name.substr(4));
        int last_log_n = parse<int>(log_entry_names.back().substr(4));

        // The third condition gurantees at most one snapshot creation per batch
        if (log_n > 0 && snapshot_period > 0 && (last_log_n - log_n) / snapshot_period == 0 && log_n % snapshot_period == 0) {
            createSnapshot();
        }
    }

    background_log_executor->scheduleAfter(500);
}

void DatabaseReplicated::writeLastExecutedToDiskAndZK() {
    current_zookeeper = getZooKeeper();
    current_zookeeper->createOrUpdate(zookeeper_path + "/replicas/" + replica_name, last_executed_log_entry, zkutil::CreateMode::Persistent);

    String metadata_file = getMetadataPath() + ".last_entry";
    WriteBufferFromFile out(metadata_file, last_executed_log_entry.size(), O_WRONLY | O_CREAT);
    writeString(last_executed_log_entry, out);
    out.next();
    if (global_context.getSettingsRef().fsync_metadata)
        out.sync();
    out.close();
}

void DatabaseReplicated::executeLogName(const String & log_entry_name) {
        String path = zookeeper_path + "/log/" + log_entry_name;
        current_zookeeper = getZooKeeper();
        String query_to_execute = current_zookeeper->get(path, {}, NULL);

        try
        {
            current_context = std::make_unique<Context>(global_context);
            current_context->getClientInfo().query_kind = ClientInfo::QueryKind::REPLICATED_LOG_QUERY;
            current_context->setCurrentDatabase(database_name);
            current_context->setCurrentQueryId(""); // generate random query_id
            executeQuery(query_to_execute, *current_context);
        }
        catch (const Exception & e)
        {
            tryLogCurrentException(log, "Query from zookeeper " + query_to_execute + " wasn't finished successfully");
            current_zookeeper->create(zookeeper_path + "/replicas/" + replica_name + "/errors/" + log_entry_name, e.what(), zkutil::CreateMode::Persistent);
        }

        LOG_DEBUG(log, "Executed query: {}", query_to_execute);
}

void DatabaseReplicated::propose(const ASTPtr & query) {
    current_zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Proposing query: {}", queryToString(query));

    {
        std::lock_guard lock(log_name_mutex);
        log_name_to_exec_with_result = current_zookeeper->create(zookeeper_path + "/log/log-", queryToString(query), zkutil::CreateMode::PersistentSequential);
    }

    background_log_executor->schedule();
}

BlockIO DatabaseReplicated::getFeedback() {
    BlockIO res;
    if (feedback_timeout == 0)
        return res;

    Stopwatch watch;

    NamesAndTypes block_structure = {
        {"replica_name", std::make_shared<DataTypeString>()},
        {"execution_feedback", std::make_shared<DataTypeString>()},
    };
    auto replica_name_column = block_structure[0].type->createColumn();
    auto feedback_column = block_structure[1].type->createColumn();

    current_zookeeper = getZooKeeper();
    Strings replica_states = current_zookeeper->getChildren(zookeeper_path + "/replicas");
    auto replica_iter = replica_states.begin();

    while (!replica_states.empty() && watch.elapsedSeconds() < feedback_timeout) {
        String last_executed = current_zookeeper->get(zookeeper_path + "/replicas/" + *replica_iter);
        if (last_executed > log_name_to_exec_with_result) {
            replica_name_column->insert(*replica_iter);
            String err_path = zookeeper_path + "/replicas/" + *replica_iter + "/errors/" + log_name_to_exec_with_result;
            if (!current_zookeeper->exists(err_path)) {
                feedback_column->insert("OK");
            } else {
                String feedback = current_zookeeper->get(err_path, {}, NULL);
                feedback_column->insert(feedback);
            }
        replica_states.erase(replica_iter);
        replica_iter = replica_states.begin();
        }
    }

    Block block = Block({
        {std::move(replica_name_column), block_structure[0].type, block_structure[0].name},
        {std::move(feedback_column), block_structure[1].type, block_structure[1].name}});

    res.in = std::make_shared<OneBlockInputStream>(block);
    return res;
}

void DatabaseReplicated::createSnapshot() {
    current_zookeeper = getZooKeeper();
    String snapshot_path = zookeeper_path + "/snapshots/" + last_executed_log_entry;

    if (Coordination::Error::ZNODEEXISTS == current_zookeeper->tryCreate(snapshot_path, String(), zkutil::CreateMode::Persistent)) {
        return;
    }
    
    for (auto iterator = getTablesIterator(global_context, {}); iterator->isValid(); iterator->next()) {
        String table_name = iterator->name();
        auto query = getCreateQueryFromMetadata(getObjectMetadataPath(table_name), true);
        String statement = queryToString(query);
        current_zookeeper->create(snapshot_path + "/" + table_name, statement, zkutil::CreateMode::Persistent);
    }
    current_zookeeper->create(snapshot_path + "/.completed", String(), zkutil::CreateMode::Persistent);

    RemoveOutdatedSnapshotsAndLog();
}

void DatabaseReplicated::loadMetadataFromSnapshot() {
    // Executes the latest snapshot.
    // Used by new replicas only.
    current_zookeeper = getZooKeeper();

    Strings snapshots;
    if (current_zookeeper->tryGetChildren(zookeeper_path + "/snapshots", snapshots) != Coordination::Error::ZOK)
        return;

    auto latest_snapshot = std::max_element(snapshots.begin(), snapshots.end());
    while (snapshots.size() > 0 && !current_zookeeper->exists(zookeeper_path + "/snapshots/" + *latest_snapshot + "/.completed")) {
        snapshots.erase(latest_snapshot);
        latest_snapshot = std::max_element(snapshots.begin(), snapshots.end());
    }

    if (snapshots.size() < 1) {
        return;
    }

    Strings metadatas;
    if (current_zookeeper->tryGetChildren(zookeeper_path + "/snapshots/" + *latest_snapshot, metadatas) != Coordination::Error::ZOK)
        return;

    LOG_DEBUG(log, "Executing {} snapshot", *latest_snapshot);

    for (auto t = metadatas.begin(); t != metadatas.end(); ++t) {
        String path = zookeeper_path + "/snapshots/" + *latest_snapshot + "/" + *t;

        String query_to_execute = current_zookeeper->get(path, {}, NULL);

        current_context = std::make_unique<Context>(global_context);
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
    current_zookeeper = getZooKeeper();
    current_zookeeper->tryRemove(zookeeper_path + "/replicas/" + replica_name);
    DatabaseAtomic::drop(context_);
}

}
