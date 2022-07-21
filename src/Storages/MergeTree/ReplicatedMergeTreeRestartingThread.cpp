#include <IO/Operators.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/randomSeed.h>
#include <boost/algorithm/string/replace.hpp>


namespace ProfileEvents
{
    extern const Event ReplicaPartialShutdown;
}

namespace CurrentMetrics
{
    extern const Metric ReadonlyReplica;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int REPLICA_IS_ALREADY_ACTIVE;
    extern const int REPLICA_STATUS_CHANGED;

}

namespace
{
    constexpr auto retry_period_ms = 10 * 1000;
}

/// Used to check whether it's us who set node `is_active`, or not.
static String generateActiveNodeIdentifier()
{
    return "pid: " + toString(getpid()) + ", random: " + toString(randomSeed());
}

ReplicatedMergeTreeRestartingThread::ReplicatedMergeTreeRestartingThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeRestartingThread)")
    , log(&Poco::Logger::get(log_name))
    , active_node_identifier(generateActiveNodeIdentifier())
{
    const auto storage_settings = storage.getSettings();
    check_period_ms = storage_settings->zookeeper_session_expiration_check_period.totalSeconds() * 1000;

    task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeRestartingThread::run()
{
    if (need_stop)
        return;

    size_t reschedule_period_ms = check_period_ms;

    try
    {
        bool replica_is_active = runImpl();
        if (!replica_is_active)
            reschedule_period_ms = retry_period_ms;
    }
    catch (const Exception & e)
    {
        /// We couldn't activate table let's set it into readonly mode
        partialShutdown();
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code() == ErrorCodes::REPLICA_STATUS_CHANGED)
            reschedule_period_ms = 0;
    }
    catch (...)
    {
        partialShutdown();
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (first_time)
    {
        if (storage.is_readonly)
        {
            /// We failed to start replication, table is still readonly, so we should increment the metric. See also setNotReadonly().
            CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
        }
        /// It does not matter if replication is actually started or not, just notify after the first attempt.
        storage.startup_event.set();
        first_time = false;
    }

    if (need_stop)
        return;

    if (reschedule_period_ms)
        task->scheduleAfter(reschedule_period_ms);
    else
        task->schedule();
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
        __builtin_unreachable();
    }

    try
    {
        storage.setZooKeeper();
    }
    catch (const Coordination::Exception &)
    {
        /// The exception when you try to zookeeper_init usually happens if DNS does not work. We will try to do it again.
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
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
    storage.part_check_thread.start();
    storage.background_operations_assignee.start();
    storage.queue_updating_task->activateAndSchedule();
    storage.mutations_updating_task->activateAndSchedule();
    storage.mutations_finalizing_task->activateAndSchedule();
    storage.merge_selecting_task->activateAndSchedule();
    if (storage.auto_optimize_partition_task)
        storage.auto_optimize_partition_task->activateAndSchedule();
    storage.cleanup_thread.start();

    return true;
}


bool ReplicatedMergeTreeRestartingThread::tryStartup()
{
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
            storage.forgetPartAndMoveToDetached(part, "noquorum");
            storage.queue.removeFailedQuorumPart(part->info);
        }
    }
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
    zookeeper->waitForEphemeralToDisappearIfAny(is_active_path);

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
    setReadonly(part_of_full_shutdown);
    ProfileEvents::increment(ProfileEvents::ReplicaPartialShutdown);

    storage.partial_shutdown_called = true;
    storage.partial_shutdown_event.set();
    storage.replica_is_active_node = nullptr;

    LOG_TRACE(log, "Waiting for threads to finish");

    storage.merge_selecting_task->deactivate();
    storage.queue_updating_task->deactivate();
    storage.mutations_updating_task->deactivate();
    storage.mutations_finalizing_task->deactivate();

    storage.cleanup_thread.stop();

    /// Stop queue processing
    {
        auto fetch_lock = storage.fetcher.blocker.cancel();
        auto merge_lock = storage.merger_mutator.merges_blocker.cancel();
        auto move_lock = storage.parts_mover.moves_blocker.cancel();
        storage.background_operations_assignee.finish();
    }

    /// Stop part_check_thread after queue processing, because some queue tasks may restart part_check_thread
    storage.part_check_thread.stop();

    LOG_TRACE(log, "Threads finished");
}


void ReplicatedMergeTreeRestartingThread::shutdown()
{
    /// Stop restarting_thread before stopping other tasks - so that it won't restart them again.
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "Restarting thread finished");

    /// Stop other tasks.
    partialShutdown(/* part_of_full_shutdown */ true);
}

void ReplicatedMergeTreeRestartingThread::setReadonly(bool on_shutdown)
{
    bool old_val = false;
    bool became_readonly = storage.is_readonly.compare_exchange_strong(old_val, true);

    /// Do not increment the metric if replica became readonly due to shutdown.
    if (became_readonly && on_shutdown)
        return;

    if (became_readonly)
        CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);

    /// Replica was already readonly, but we should decrement the metric, because we are detaching/dropping table.
    if (on_shutdown)
        CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
}

void ReplicatedMergeTreeRestartingThread::setNotReadonly()
{
    bool old_val = true;
    /// is_readonly is true on startup, but ReadonlyReplica metric is not incremented,
    /// because we don't want to change this metric if replication is started successfully.
    /// So we should not decrement it when replica stopped being readonly on startup.
    if (storage.is_readonly.compare_exchange_strong(old_val, false) && !first_time)
        CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
}

}
