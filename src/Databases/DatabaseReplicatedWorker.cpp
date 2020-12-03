#include <Databases/DatabaseReplicatedWorker.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/DDLTask.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DatabaseReplicatedDDLWorker::DatabaseReplicatedDDLWorker(DatabaseReplicated * db, const Context & context_)
    : DDLWorker(/* pool_size */ 1, db->zookeeper_path + "/log", context_, nullptr, {}, fmt::format("DDLWorker({})", db->getDatabaseName()))
    , database(db)
{
    /// Pool size must be 1 (to avoid reordering of log entries)
}

void DatabaseReplicatedDDLWorker::initializeMainThread()
{
    do
    {
        try
        {
            auto zookeeper = getAndSetZooKeeper();
            initializeReplication();
            initialized = true;
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Error on initialization of {}", database->getDatabaseName()));
            sleepForSeconds(5);
        }
    }
    while (!initialized && !stop_flag);
}

void DatabaseReplicatedDDLWorker::initializeReplication()
{
    /// Check if we need to recover replica.
    /// Invariant: replica is lost if it's log_ptr value is less then min_log_ptr value.

    UInt32 our_log_ptr = parse<UInt32>(current_zookeeper->get(database->replica_path + "/log_ptr"));
    UInt32 min_log_ptr = parse<UInt32>(current_zookeeper->get(database->zookeeper_path + "/min_log_ptr"));
    if (our_log_ptr < min_log_ptr)
        database->recoverLostReplica(current_zookeeper, 0);
}

String DatabaseReplicatedDDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    auto zookeeper = getAndSetZooKeeper();
    const String query_path_prefix = queue_dir + "/query-";

    /// We cannot create sequential node and it's ephemeral child in a single transaction, so allocate sequential number another way
    String counter_prefix = database->zookeeper_path + "/counter/cnt-";
    String counter_path = zookeeper->create(counter_prefix, "", zkutil::CreateMode::EphemeralSequential);
    String node_path = query_path_prefix + counter_path.substr(counter_prefix.size());

    Coordination::Requests ops;
    /// Query is not committed yet, but we have to write it into log to avoid reordering
    ops.emplace_back(zkutil::makeCreateRequest(node_path, entry.toString(), zkutil::CreateMode::Persistent));
    /// '/try' will be replaced with '/committed' or will be removed due to expired session or other error
    ops.emplace_back(zkutil::makeCreateRequest(node_path + "/try", database->getFullReplicaName(), zkutil::CreateMode::Ephemeral));
    /// We don't need it anymore
    ops.emplace_back(zkutil::makeRemoveRequest(counter_path, -1));
    /// Create status dirs
    ops.emplace_back(zkutil::makeCreateRequest(node_path + "/active", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(node_path + "/finished", "", zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);

    return node_path;
}

DDLTaskPtr DatabaseReplicatedDDLWorker::initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper)
{
    UInt32 our_log_ptr = parse<UInt32>(current_zookeeper->get(database->replica_path + "/log_ptr"));
    UInt32 entry_num = DatabaseReplicatedTask::getLogEntryNumber(entry_name);

    if (entry_num <= our_log_ptr)
    {
        out_reason = fmt::format("Task {} already executed according to log pointer {}", entry_name, our_log_ptr);
        return {};
    }

    String entry_path = queue_dir + "/" + entry_name;
    auto task = std::make_unique<DatabaseReplicatedTask>(entry_name, entry_path, database);

    String initiator_name;
    zkutil::EventPtr wait_committed_or_failed = std::make_shared<Poco::Event>();

    if (zookeeper->tryGet(entry_path + "/try", initiator_name, nullptr, wait_committed_or_failed))
    {
        task->we_are_initiator = initiator_name == task->host_id_str;
        /// Query is not committed yet. We cannot just skip it and execute next one, because reordering may break replication.
        //FIXME add some timeouts
        if (!task->we_are_initiator)
        {
            LOG_TRACE(log, "Waiting for initiator {} to commit or rollback entry {}", initiator_name, entry_path);
            wait_committed_or_failed->wait();
        }
    }

    if (!task->we_are_initiator && !zookeeper->exists(entry_path + "/committed"))
    {
        out_reason = "Entry " + entry_name + " hasn't been committed";
        return {};
    }

    String node_data;
    if (!zookeeper->tryGet(entry_path, node_data))
    {
        LOG_ERROR(log, "Cannot get log entry {}", entry_path);
        database->onUnexpectedLogEntry(entry_name, zookeeper);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "should be unreachable");
    }

    task->entry.parse(node_data);

    if (task->entry.query.empty())
    {
        //TODO better way to determine special entries
        out_reason = "It's dummy task";
        return {};
    }

    task->parseQueryFromEntry(context);

    if (zookeeper->exists(task->getFinishedNodePath()))
    {
        out_reason = "Task has been already processed";
        return {};
    }

    return task;
}

}
