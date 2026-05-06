#include <Storages/MergeTree/SelectiveReplication/MigrationMonitor.h>

#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/SelectiveReplication/MigrationCoordinator.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Poco/Event.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_auto_rebalance;
}

namespace SelectiveReplication
{

MigrationMonitor::MigrationMonitor(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(storage_.log.load())
{
}

void MigrationMonitor::run()
{
    auto component_guard = Coordination::setCurrentComponent("SelectiveReplication::MigrationMonitor::run");
    try
    {
        if (!storage.isSelectiveReplicationEnabled())
            return;

        /// Ensure counts node is initialized (idempotent, CAS-create).
        auto zk = storage.getZooKeeper();
        storage.replica_assignment->initializeCounts(zk);

        /// --- Local part cleanup for unassigned partitions ---
        /// After a partition migrates away (SWITCH completes), our local parts are still Active.
        /// Mark them as Outdated so the standard cleanup pipeline removes them.
        {
            /// Step 1+2: Get a fresh snapshot of all assignments from ZK.
            auto assignments = storage.replica_assignment->getAssignments(zk, {}, /*force_refresh=*/true);

            if (!assignments.empty())
            {
                /// Step 3: Scan all local Active parts (regular + patch).
                DataPartsVector parts_to_remove;

                auto regular_parts = storage.getDataPartsVectorForInternalUsage();
                auto patch_parts = storage.getPatchPartsVectorForInternalUsage();

                auto collect_unassigned = [&](const DataPartsVector & parts)
                {
                    for (const auto & part : parts)
                    {
                        /// For patch parts, use the original (non-prefixed) partition_id.
                        String partition_id = part->info.getOriginalPartitionId();

                        auto it = assignments.find(partition_id);
                        if (it == assignments.end())
                        {
                            /// Partition not in assignment snapshot — legacy data from before
                            /// selective replication was enabled. Keep it (fallback data).
                            continue;
                        }

                        /// Check if this replica is in the assigned list (strip :cloning suffix).
                        const auto & assigned = it->second.replicas;
                        bool replica_is_assigned = KeeperReplicaAssignment::isReplicaAssigned(assigned, storage.replica_name);

                        if (!replica_is_assigned)
                        {
                            /// Partition exists in assignments but is not assigned to this replica.
                            parts_to_remove.push_back(part);
                        }
                    }
                };

                collect_unassigned(regular_parts);
                collect_unassigned(patch_parts);

                /// Bug #7 fix: Re-verify assignment from ZK for each part before
                /// removal. Between the snapshot above and removePartsFromWorkingSet,
                /// a new assignment could move the partition back to this replica.
                if (!parts_to_remove.empty())
                {
                    std::unordered_set<String> pids_to_check;
                    for (const auto & part : parts_to_remove)
                        pids_to_check.insert(part->info.getOriginalPartitionId());

                    auto fresh_assignments = storage.replica_assignment->getAssignments(
                        zk, std::vector<String>(pids_to_check.begin(), pids_to_check.end()), /*force_refresh=*/true);

                    DataPartsVector verified_parts;
                    for (const auto & part : parts_to_remove)
                    {
                        String partition_id = part->info.getOriginalPartitionId();
                        auto it = fresh_assignments.find(partition_id);
                        if (it == fresh_assignments.end())
                            continue; // partition disappeared from assignments — keep data

                        if (!KeeperReplicaAssignment::isReplicaAssigned(it->second.replicas, storage.replica_name))
                            verified_parts.push_back(part);
                    }

                    if (!verified_parts.empty())
                    {
                        /// Final CAS guard: verify that assignment versions haven't changed
                        /// between the fresh read above and the actual removal. A concurrent
                        /// migration SWITCH updates the assignment node with a CAS write, so
                        /// any version change means the partition may have been re-assigned
                        /// back to this replica. If any version changed, skip the entire
                        /// removal batch (conservative — retry next cycle).
                        Coordination::Requests check_ops;
                        std::unordered_set<String> checked_pids;
                        String assignments_base = fs::path(storage.zookeeper_path) / "selective" / "assignments";
                        for (const auto & part : verified_parts)
                        {
                            String pid = part->info.getOriginalPartitionId();
                            if (!checked_pids.insert(pid).second)
                                continue;
                            auto it = fresh_assignments.find(pid);
                            if (it != fresh_assignments.end() && it->second.version >= 0)
                                check_ops.emplace_back(zkutil::makeCheckRequest(
                                    fs::path(assignments_base) / pid, it->second.version));
                        }

                        bool versions_ok = true;
                        if (!check_ops.empty())
                        {
                            Coordination::Responses responses;
                            auto rc = zk->tryMulti(check_ops, responses);
                            if (rc != Coordination::Error::ZOK)
                            {
                                LOG_INFO(log, "Assignment versions changed during cleanup verification, "
                                    "skipping removal of {} parts (will retry next cycle)", verified_parts.size());
                                versions_ok = false;
                            }
                        }

                        if (versions_ok)
                        {
                            LOG_INFO(log, "Cleaning up {} parts from unassigned partitions (verified)", verified_parts.size());
                            storage.removePartsFromWorkingSet(/*txn=*/ nullptr, verified_parts, /*clear_without_timeout=*/ true);
                        }
                    }
                }
            }
        }

        runMigrationCycle(/*force_rebalance=*/false);
    }
    catch (const DB::Exception & e)
    {
        tryLogCurrentException(log, "Failed in migration monitor task");

        /// Re-throw non-recoverable exceptions to avoid silently masking programming errors.
        if (e.code() == ErrorCodes::LOGICAL_ERROR
            || e.code() == ErrorCodes::ABORTED)
            throw;
    }

    storage.migration_monitor_task->scheduleAfter(
        SelectiveReplication::MIGRATION_MONITOR_INTERVAL_SECONDS * 1000);
}

void MigrationMonitor::runMigrationCycle(bool force_rebalance)
{
    PartitionMigrationCoordinator coordinator(storage);

    /// Drive state machine for our own migrations (as coordinator).
    coordinator.driveMigrations(/*own_only=*/true);

    /// Allow target replicas to check and signal clone completion.
    coordinator.checkAndSignalCloneComplete();

    /// Recover orphaned migrations (where coordinator went offline).
    coordinator.recoverOrphanedMigrations();

    /// Check balance and start new migrations if requested or auto-rebalance is enabled.
    if (force_rebalance || (*storage.getSettings())[MergeTreeSetting::enable_auto_rebalance])
        coordinator.checkAndStartAutoRebalance();
}

void MigrationMonitor::triggerRebalance()
{
    if (!storage.isSelectiveReplicationEnabled())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "SYSTEM START SELECTIVE REBALANCE is only supported for tables with selective replication "
            "(replication_factor > 0)");

    auto component_guard = Coordination::setCurrentComponent("SelectiveReplication::MigrationMonitor::triggerRebalance");

    runMigrationCycle(/*force_rebalance=*/true);
}

bool MigrationMonitor::waitForMigrationsComplete(UInt64 timeout_ms)
{
    if (!storage.isSelectiveReplicationEnabled())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "SYSTEM SYNC SELECTIVE MIGRATIONS is only supported for tables with selective replication "
            "(replication_factor > 0)");

    auto component_guard = Coordination::setCurrentComponent("SelectiveReplication::MigrationMonitor::waitForMigrationsComplete");

    const String migrations_path = fs::path(storage.zookeeper_path) / "selective" / SelectiveReplication::MIGRATIONS_SUBPATH;
    auto start = std::chrono::steady_clock::now();

    auto event = std::make_shared<Poco::Event>();

    while (true)
    {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (static_cast<UInt64>(elapsed_ms) >= timeout_ms)
            return false;

        try
        {
            auto zk = storage.getZooKeeper();

            /// Set a watch on the migrations directory so we wake up on child changes
            /// (e.g., migration node removed on completion) instead of polling.
            Coordination::Stat stat;
            Strings children;
            auto code = zk->tryGetChildrenWatch(migrations_path, children, &stat, event);
            if (code != Coordination::Error::ZOK)
                return children.empty();

            auto active = PartitionMigrationCoordinator::getActiveMigrationPartitions(zk, migrations_path);
            if (active.empty())
                return true;

            PartitionMigrationCoordinator coordinator(storage);
            coordinator.driveMigrations(/*own_only=*/false);
            coordinator.checkAndSignalCloneComplete();
            coordinator.recoverOrphanedMigrations();
        }
        catch (const Coordination::Exception & e)
        {
            /// ZooKeeper session may expire or lose connection transiently.
            /// Log and retry until timeout rather than propagating the exception.
            LOG_WARNING(log, "ZooKeeper exception while waiting for selective migrations, will retry: {}", e.message());
        }

        auto elapsed_since_start = static_cast<UInt64>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count());
        if (elapsed_since_start >= timeout_ms)
            return false;
        auto remaining_ms = timeout_ms - elapsed_since_start;
        auto wait_ms = std::min(remaining_ms, static_cast<UInt64>(2000));
        event->tryWait(wait_ms);
        event->reset();
    }
}

}
}
