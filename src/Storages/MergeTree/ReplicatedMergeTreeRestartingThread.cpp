#include <atomic>
#include <IO/Operators.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Interpreters/Context.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerUUID.h>
#include <boost/algorithm/string/replace.hpp>


namespace CurrentMetrics
{
    extern const Metric ReadonlyReplica;
}


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds zookeeper_session_expiration_check_period;
}

namespace ErrorCodes
{
    extern const int REPLICA_IS_ALREADY_ACTIVE;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace FailPoints
{
    extern const char finish_clean_quorum_failed_parts[];
};

/// Used to check whether it's us who set node `is_active`, or not.
static String generateActiveNodeIdentifier()
{
    return Field(ServerUUID::get()).dump();
}

ReplicatedMergeTreeRestartingThread::ReplicatedMergeTreeRestartingThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeRestartingThread)")
    , log(getLogger(log_name))
    , active_node_identifier(generateActiveNodeIdentifier())
{
    const auto storage_settings = storage.getSettings();
    check_period_ms = (*storage_settings)[MergeTreeSetting::zookeeper_session_expiration_check_period].totalSeconds() * 1000;

    task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeRestartingThread::start(bool schedule)
{
    LOG_TRACE(log, "Starting the restarting thread, schedule: {}", schedule);
    if (schedule)
        task->activateAndSchedule();
    else
        task->activate();
}

void ReplicatedMergeTreeRestartingThread::wakeup()
{
    task->schedule();
}

void ReplicatedMergeTreeRestartingThread::run()
{
    if (need_stop)
        return;

    /// In case of any exceptions we want to rerun the this task as fast as possible but we also don't want to keep retrying immediately
    /// in a close loop (as fast as tasks can be processed), so we'll retry in between 100 and 10000 ms
    const size_t backoff_ms = 100 * ((consecutive_check_failures + 1) * (consecutive_check_failures + 2)) / 2;
    const size_t next_failure_retry_ms = std::min(size_t{10000}, backoff_ms);

    try
    {
        bool replica_is_active = runImpl();
        if (replica_is_active)
        {
            consecutive_check_failures = 0;
            task->scheduleAfter(check_period_ms);
        }
        else
        {
            consecutive_check_failures++;
            task->scheduleAfter(next_failure_retry_ms);
        }
    }
    catch (...)
    {
        consecutive_check_failures++;
        task->scheduleAfter(next_failure_retry_ms);

        /// We couldn't activate table let's set it into readonly mode if necessary
        /// We do this after scheduling the task in case it throws
        partialShutdown();
        tryLogCurrentException(log, "Failed to restart the table. Will try again");
    }

    if (first_time)
    {
        if (storage.is_readonly && !std::exchange(storage.is_readonly_metric_set, true))
            /// We failed to start replication, table is still readonly, so we should increment the metric. See also setNotReadonly().
            CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);

        /// It does not matter if replication is actually started or not, just notify after the first attempt.
        storage.startup_event.set();
        first_time = false;
    }
}

bool ReplicatedMergeTreeRestartingThread::runImpl()
{
    if (!storage.is_readonly && !storage.getZooKeeper()->expired())
        return true;

    if (first_time)
    {
        LOG_DEBUG(log, "Activating replica.");
        assert(storage.is_readonly);
    }
    else if (storage.is_readonly)
    {
        LOG_WARNING(log, "Table was in readonly mode. Will try to activate it.");
    }
    else if (storage.getZooKeeper()->expired())
    {
        LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");
        partialShutdown();
    }
    else
    {
        UNREACHABLE();
    }

    try
    {
        storage.setZooKeeper();
    }
    catch (const Coordination::Exception &)
    {
        /// The exception when you try to zookeeper_init usually happens if DNS does not work or the connection with ZK fails
        tryLogCurrentException(log, "Failed to establish a new ZK connection. Will try again");
        assert(storage.is_readonly);
        return false;
    }

    if (need_stop)
        return false;

    if (!tryStartup())
    {
        assert(storage.is_readonly);
        return false;
    }

    setNotReadonly();

    /// Start queue processing
    storage.background_operations_assignee.start();
    storage.queue_updating_task->activateAndSchedule();
    storage.mutations_updating_task->activateAndSchedule();
    storage.mutations_finalizing_task->activateAndSchedule();
    storage.merge_selecting_task->activateAndSchedule();
    storage.cleanup_thread.start();
    storage.async_block_ids_cache.start();
    storage.part_check_thread.start();

    LOG_DEBUG(log, "Table started successfully");
    return true;
}


bool ReplicatedMergeTreeRestartingThread::tryStartup()
{
    LOG_DEBUG(log, "Trying to start replica up");
    try
    {
        removeFailedQuorumParts();
        activateReplica();

        const auto & zookeeper = storage.getZooKeeper();
        const auto storage_settings = storage.getSettings();

        storage.cloneReplicaIfNeeded(zookeeper);

        try
        {
            storage.queue.initialize(zookeeper);
            storage.queue.load(zookeeper);
            storage.queue.createLogEntriesToFetchBrokenParts();

            /// pullLogsToQueue() after we mark replica 'is_active' (and after we repair if it was lost);
            /// because cleanup_thread doesn't delete log_pointer of active replicas.
            storage.queue.pullLogsToQueue(zookeeper, {}, ReplicatedMergeTreeQueue::LOAD);
        }
        catch (...)
        {
            std::lock_guard lock(storage.last_queue_update_exception_lock);
            storage.last_queue_update_exception = getCurrentExceptionMessage(false);
            throw;
        }

        const Int32 replica_metadata_version = fixReplicaMetadataVersionIfNeeded(zookeeper);
        const bool replica_metadata_version_exists = replica_metadata_version != -1;
        if (replica_metadata_version_exists)
        {
            storage.setInMemoryMetadata(storage.getInMemoryMetadataPtr()->withMetadataVersion(replica_metadata_version));
        }
        else
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "It seems you have upgraded from a version earlier than 20.4 straight to one later than 24.10. "
                "ClickHouse does not support upgrades that span more than a year. "
                "Please update gradually (through intermediate versions).");
        }

        storage.queue.removeCurrentPartsFromMutations();
        storage.last_queue_update_finish_time.store(time(nullptr));

        updateQuorumIfWeHavePart();

        /// Anything above can throw a KeeperException if something is wrong with ZK.
        /// Anything below should not throw exceptions.

        storage.partial_shutdown_called = false;
        storage.partial_shutdown_event.reset();
        return true;
    }
    catch (...)
    {
        storage.replica_is_active_node = nullptr;

        try
        {
            throw;
        }
        catch (const Coordination::Exception & e)
        {
            LOG_ERROR(log, "Couldn't start replication (table will be in readonly mode): {}. {}", e.what(), DB::getCurrentExceptionMessage(true));
            return false;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::REPLICA_IS_ALREADY_ACTIVE)
                throw;

            LOG_ERROR(log, "Couldn't start replication (table will be in readonly mode): {}. {}", e.what(), DB::getCurrentExceptionMessage(true));
            return false;
        }
    }
}


void ReplicatedMergeTreeRestartingThread::removeFailedQuorumParts()
{
    auto zookeeper = storage.getZooKeeper();

    Strings failed_parts;
    if (zookeeper->tryGetChildren(storage.zookeeper_path + "/quorum/failed_parts", failed_parts) != Coordination::Error::ZOK)
        return;

    /// Firstly, remove parts from ZooKeeper
    storage.removePartsFromZooKeeperWithRetries(failed_parts);

    for (const auto & part_name : failed_parts)
    {
        auto part = storage.getPartIfExists(
            part_name, {MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

        if (part)
        {
            LOG_DEBUG(log, "Found part {} with failed quorum. Moving to detached. This shouldn't happen often.", part_name);
            storage.forcefullyMovePartToDetachedAndRemoveFromMemory(part, "noquorum");
            storage.queue.removeFailedQuorumPart(part->info);
        }
    }
    FailPointInjection::disableFailPoint(FailPoints::finish_clean_quorum_failed_parts);
}


void ReplicatedMergeTreeRestartingThread::updateQuorumIfWeHavePart()
{
    auto zookeeper = storage.getZooKeeper();

    String quorum_str;
    if (zookeeper->tryGet(fs::path(storage.zookeeper_path) / "quorum" / "status", quorum_str))
    {
        ReplicatedMergeTreeQuorumEntry quorum_entry(quorum_str);

        if (!quorum_entry.replicas.contains(storage.replica_name)
            && storage.getActiveContainingPart(quorum_entry.part_name))
        {
            LOG_WARNING(log, "We have part {} but we is not in quorum. Updating quorum. This shouldn't happen often.", quorum_entry.part_name);
            storage.updateQuorum(quorum_entry.part_name, false);
        }
    }

    Strings part_names;
    String parallel_quorum_parts_path = fs::path(storage.zookeeper_path) / "quorum" / "parallel";
    if (zookeeper->tryGetChildren(parallel_quorum_parts_path, part_names) == Coordination::Error::ZOK)
    {
        for (auto & part_name : part_names)
        {
            if (zookeeper->tryGet(fs::path(parallel_quorum_parts_path) / part_name, quorum_str))
            {
                ReplicatedMergeTreeQuorumEntry quorum_entry(quorum_str);
                if (!quorum_entry.replicas.contains(storage.replica_name)
                    && storage.getActiveContainingPart(part_name))
                {
                    LOG_WARNING(log, "We have part {} but we is not in quorum. Updating quorum. This shouldn't happen often.", part_name);
                    storage.updateQuorum(part_name, true);
                }
            }
        }
    }
}


void ReplicatedMergeTreeRestartingThread::activateReplica()
{
    auto zookeeper = storage.getZooKeeper();

    /// How other replicas can access this one.
    ReplicatedMergeTreeAddress address = storage.getReplicatedMergeTreeAddress();

    String is_active_path = fs::path(storage.replica_path) / "is_active";
    zookeeper->deleteEphemeralNodeIfContentMatches(is_active_path, active_node_identifier);

    /// Simultaneously declare that this replica is active, and update the host.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(is_active_path, active_node_identifier, zkutil::CreateMode::Ephemeral));
    ops.emplace_back(zkutil::makeSetRequest(fs::path(storage.replica_path) / "host", address.toString(), -1));

    try
    {
        zookeeper->multi(ops);
    }
    catch (const Coordination::Exception & e)
    {
        String existing_replica_host;
        zookeeper->tryGet(fs::path(storage.replica_path) / "host", existing_replica_host);

        if (existing_replica_host.empty())
            existing_replica_host = "without host node";
        else
            boost::replace_all(existing_replica_host, "\n", ", ");

        if (e.code == Coordination::Error::ZNODEEXISTS)
            throw Exception(ErrorCodes::REPLICA_IS_ALREADY_ACTIVE,
                "Replica {} appears to be already active ({}). If you're sure it's not, "
                "try again in a minute or remove znode {}/is_active manually",
                storage.replica_path, existing_replica_host, storage.replica_path);

        throw;
    }

    /// `current_zookeeper` lives for the lifetime of `replica_is_active_node`,
    ///  since before changing `current_zookeeper`, `replica_is_active_node` object is destroyed in `partialShutdown` method.
    storage.replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *storage.current_zookeeper);
}


void ReplicatedMergeTreeRestartingThread::partialShutdown(bool part_of_full_shutdown)
{
    setReadonly(/* on_shutdown = */ part_of_full_shutdown);
    storage.partialShutdown();
}


void ReplicatedMergeTreeRestartingThread::shutdown(bool part_of_full_shutdown)
{
    /// Stop restarting_thread before stopping other tasks - so that it won't restart them again.
    need_stop = part_of_full_shutdown;
    task->deactivate();

    /// Explicitly set the event, because the restarting thread will not set it again
    if (part_of_full_shutdown)
        storage.startup_event.set();

    LOG_TRACE(log, "Restarting thread finished");

    setReadonly(part_of_full_shutdown);

}

void ReplicatedMergeTreeRestartingThread::setReadonly(bool on_shutdown)
{
    bool old_val = false;
    bool became_readonly = storage.is_readonly.compare_exchange_strong(old_val, true);

    if (became_readonly)
    {
        const UInt32 now = static_cast<UInt32>(
            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
        storage.readonly_start_time.store(now, std::memory_order_relaxed);
    }

    /// Do not increment the metric if replica became readonly due to shutdown.
    if (became_readonly && on_shutdown)
        return;

    if (became_readonly)
    {
        chassert(!storage.is_readonly_metric_set);
        storage.is_readonly_metric_set = true;
        CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
        return;
    }

    /// Replica was already readonly, but we should decrement the metric if it was set because we are detaching/dropping table.
    /// the task should be deactivated if it's full shutdown so no race is present
    if (on_shutdown && std::exchange(storage.is_readonly_metric_set, false))
    {
        CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
        chassert(CurrentMetrics::get(CurrentMetrics::ReadonlyReplica) >= 0);
    }
}

void ReplicatedMergeTreeRestartingThread::setNotReadonly()
{
    bool old_val = true;
    /// is_readonly is true on startup, but ReadonlyReplica metric is not incremented,
    /// because we don't want to change this metric if replication is started successfully.
    /// So we should not decrement it when replica stopped being readonly on startup.
    if (storage.is_readonly.compare_exchange_strong(old_val, false) && std::exchange(storage.is_readonly_metric_set, false))
    {
        CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
        chassert(CurrentMetrics::get(CurrentMetrics::ReadonlyReplica) >= 0);
    }

    storage.readonly_start_time.store(0, std::memory_order_relaxed);
}


Int32 ReplicatedMergeTreeRestartingThread::fixReplicaMetadataVersionIfNeeded(zkutil::ZooKeeperPtr zookeeper)
{
    const String & zookeeper_path = storage.zookeeper_path;
    const String & replica_path = storage.replica_path;

    const size_t num_attempts = 2;
    for (size_t attempt = 0; attempt != num_attempts; ++attempt)
    {
        String replica_metadata_version_str;
        Coordination::Stat replica_stat;
        const bool replica_metadata_version_exists = zookeeper->tryGet(replica_path + "/metadata_version", replica_metadata_version_str, &replica_stat);
        if (!replica_metadata_version_exists)
            return -1;

        const Int32 metadata_version = parse<Int32>(replica_metadata_version_str);
        if (metadata_version != 0)
            return metadata_version;

        Coordination::Stat table_stat;
        zookeeper->get(fs::path(zookeeper_path) / "metadata", &table_stat);
        if (table_stat.version == 0)
            return metadata_version;

        ReplicatedMergeTreeQueue & queue = storage.queue;
        queue.pullLogsToQueue(zookeeper, {}, ReplicatedMergeTreeQueue::FIX_METADATA_VERSION);
        if (queue.getStatus().metadata_alters_in_queue != 0)
        {
            LOG_INFO(log, "Skipping updating metadata_version as there are ALTER_METADATA entries in the queue");
            return metadata_version;
        }

        const Coordination::Requests ops = {
            zkutil::makeSetRequest(fs::path(replica_path) / "metadata_version", std::to_string(table_stat.version), replica_stat.version),
            zkutil::makeCheckRequest(fs::path(zookeeper_path) / "metadata", table_stat.version),
        };
        Coordination::Responses ops_responses;
        const Coordination::Error code = zookeeper->tryMulti(ops, ops_responses);
        if (code == Coordination::Error::ZOK)
        {
            LOG_DEBUG(log, "Successfully set metadata_version to {}", table_stat.version);
            return table_stat.version;
        }

        if (code == Coordination::Error::ZBADVERSION)
        {
            LOG_WARNING(log, "Cannot fix metadata_version because either metadata.version or metadata_version.version changed, attempts left = {}", num_attempts - attempt - 1);
            continue;
        }

        throw zkutil::KeeperException(code);
    }

    /// Second attempt is only possible if either metadata_version.version or metadata.version changed during the first attempt.
    /// If metadata_version changed to non-zero value during the first attempt, on second attempt we will return the new metadata_version.
    /// If metadata.version changed during first attempt, on second attempt we will either get metadata_version != 0 and return the new metadata_version or we will get metadata_alters_in_queue != 0 and return 0.
    /// So either first or second attempt should return unless metadata_version was rewritten from 0 to 0 during the first attempt which is highly unlikely.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to fix replica metadata_version in ZooKeeper after two attempts");
}

}
