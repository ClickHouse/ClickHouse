#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/SelectiveReplication/Router.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/ReplicatedMergeTreePartsCollector.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace CurrentMetrics
{
    extern const Metric OptimizeForwardingThreads;
    extern const Metric OptimizeForwardingThreadsActive;
    extern const Metric OptimizeForwardingThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event SelectiveReplicationOptimizeForwardedCommands;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_LOST;
}

class StorageReplicatedMergeTree;

namespace SelectiveReplication
{

/// Forwards OPTIMIZE commands to replicas assigned to each partition
/// under selective replication. Extracted from
/// `StorageReplicatedMergeTree` to keep forwarding logic focused,
/// following the `MergeTreeDataSelectExecutor` pattern: holds a storage
/// reference and is declared as `friend class` in
/// `StorageReplicatedMergeTree`.
class OptimizeForwarder
{
public:
    explicit OptimizeForwarder(StorageReplicatedMergeTree & storage_);

    /// Run OPTIMIZE with selective routing: execute locally for owned
    /// partitions, forward to the assigned replica for others.
    ///
    /// `handle_noop` is a callable with signature
    /// `bool(FormatStringHelper<Args...>, Args&&...)` used to report
    /// "nothing to merge" conditions (either log + return false, or throw).
    template <typename HandleNoop>
    bool optimizeWithSelectiveRouting(
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr query_context,
        HandleNoop && handle_noop);

    /// Forward a single OPTIMIZE command to a specific remote replica.
    bool forwardOptimizeToReplica(
        const String & target_replica,
        const String & partition_id,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        const ReplicatedMergeTreeAddress & address,
        ContextPtr query_context);

private:
    StorageReplicatedMergeTree & storage;
    LoggerPtr log;

    /// Helpers that need full Context / Settings definitions — defined in .cpp
    /// so the header can avoid broad includes.
    static UInt64 getMaxBytesToMergeAtMaxSpaceInPool(const MergeTreeSettingsPtr & settings);
    static bool getAssignPartUuids(const MergeTreeSettingsPtr & settings);
    static bool getOptimizeSkipMergedPartitions(ContextPtr query_context);
    static UInt64 getReceiveTimeoutMs(ContextPtr query_context);
    static UInt64 getMaxDistributedConnections(ContextPtr query_context);
    static bool getSkipUnavailableShards(ContextPtr query_context);
};


template <typename HandleNoop>
bool OptimizeForwarder::optimizeWithSelectiveRouting(
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr query_context,
    HandleNoop && handle_noop)
{
    auto zookeeper = storage.getZooKeeperAndAssertNotReadonly();
    const auto storage_settings_ptr = storage.getSettings();
    auto metadata_snapshot = storage.getInMemoryMetadataPtr(query_context, false);
    std::vector<ReplicatedMergeTreeLogEntryData> merge_entries;

    auto try_assign_merge = [&](const String & partition_id, bool explicit_partition) -> bool
    {
        constexpr size_t max_retries = 10;
        size_t try_no = 0;
        for (; try_no < max_retries; ++try_no)
        {
            std::lock_guard merge_selecting_lock(storage.merge_selecting_mutex);
            PartitionIdsHint partition_ids_hint;
            if (partition_id.empty())
            {
                partition_ids_hint = storage.getAllPartitionIds();
            }
            else
            {
                auto parts_lock = storage.readLockParts();
                if (!storage.getAnyPartInPartition(partition_id, parts_lock))
                    return handle_noop("Cannot select parts for optimization: there are no parts in partition {}", partition_id);
                partition_ids_hint.insert(partition_id);
            }

            auto merge_predicate = storage.queue.getMergePredicate(zookeeper, std::move(partition_ids_hint));
            auto parts_collector = std::make_shared<ReplicatedMergeTreePartsCollector>(storage, merge_predicate);

            const auto select_merge = [&]() -> std::expected<MergeSelectorChoices, SelectMergeFailure>
            {
                if (partition_id.empty())
                {
                    UInt64 max_source_parts_bytes_for_merge = getMaxBytesToMergeAtMaxSpaceInPool(storage_settings_ptr);
                    UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(storage);

                    return storage.merger_mutator.selectPartsToMerge(
                        parts_collector,
                        merge_predicate,
                        MergeSelectorApplier(
                            /*merge_constraints=*/{{max_source_parts_bytes_for_merge, max_result_part_rows}},
                            /*merge_with_ttl_allowed=*/false,
                            /*aggressive=*/true,
                            /*range_filter_=*/nullptr
                        ),
                        /*partitions_hint=*/std::nullopt);
                }
                else if (final)
                {
                    /// FINAL must merge all parts in the partition; bounded selectPartsToMerge
                    /// would return "no need to merge" for single level-0 parts.
                    return storage.merger_mutator.selectAllPartsToMergeWithinPartition(
                        metadata_snapshot,
                        parts_collector,
                        merge_predicate,
                        partition_id,
                        final,
                        getOptimizeSkipMergedPartitions(query_context));
                }
                else if (!explicit_partition)
                {
                    /// Non-FINAL auto-iteration: bounded merge respects max_parts_to_merge_at_once.
                    UInt64 max_source_parts_bytes_for_merge = getMaxBytesToMergeAtMaxSpaceInPool(storage_settings_ptr);
                    UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(storage);
                    PartitionIdsHint hint;
                    hint.insert(partition_id);

                    return storage.merger_mutator.selectPartsToMerge(
                        parts_collector,
                        merge_predicate,
                        MergeSelectorApplier(
                            /*merge_constraints=*/{{max_source_parts_bytes_for_merge, max_result_part_rows}},
                            /*merge_with_ttl_allowed=*/false,
                            /*aggressive=*/true,
                            /*range_filter_=*/nullptr
                        ),
                        hint);
                }
                else
                {
                    /// User specified PARTITION — merge all parts.
                    return storage.merger_mutator.selectAllPartsToMergeWithinPartition(
                        metadata_snapshot,
                        parts_collector,
                        merge_predicate,
                        partition_id,
                        final,
                        getOptimizeSkipMergedPartitions(query_context));
                }
            };

            const auto construct_future_part = [&](MergeSelectorChoices choices) -> std::expected<FutureMergedMutatedPartPtr, SelectMergeFailure>
            {
                chassert(choices.size() == 1);
                MergeSelectorChoice choice = std::move(choices[0]);

                auto future_part = constructFuturePart(storage, choice, {MergeTreeDataPartState::Active});
                if (!future_part)
                    return std::unexpected(SelectMergeFailure{
                        .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                        .explanation = PreformattedMessage::create("Can't construct future part from source parts. Probably there was a drop part/partition user query."),
                    });

                if (getAssignPartUuids(storage_settings_ptr))
                    future_part->uuid = UUIDHelpers::generateV4();

                return future_part;
            };

            auto select_merge_result = select_merge().and_then(construct_future_part);

            if (!select_merge_result.has_value())
            {
                auto error = std::move(select_merge_result.error());

                if (error.reason == SelectMergeFailure::Reason::NOTHING_TO_MERGE)
                    return false;

                if (try_no + 1 < max_retries)
                {
                    const auto wait_timeout = getReceiveTimeoutMs(query_context) / max_retries;
                    storage.waitForProcessingQueue(wait_timeout, SyncReplicaMode::DEFAULT, {});
                    continue;
                }

                if (!partition_id.empty())
                    error.explanation.text += fmt::format(" (in partition {})", partition_id);

                return handle_noop("Cannot select parts for optimization: {}", error.explanation.text);
            }

            ReplicatedMergeTreeLogEntryData merge_entry;
            StorageReplicatedMergeTree::CreateMergeEntryResult create_result = storage.createLogEntryToMergeParts(
                zookeeper,
                select_merge_result.value()->parts,
                select_merge_result.value()->patch_parts,
                select_merge_result.value()->name,
                select_merge_result.value()->uuid,
                select_merge_result.value()->part_format,
                deduplicate,
                deduplicate_by_columns,
                cleanup,
                &merge_entry,
                merge_predicate->getVersion(),
                select_merge_result.value()->merge_type);

            if (create_result == StorageReplicatedMergeTree::CreateMergeEntryResult::MissingPart)
                return handle_noop("Can't create merge queue node in ZooKeeper, because some parts are missing");

            if (create_result == StorageReplicatedMergeTree::CreateMergeEntryResult::LogUpdated)
                continue;

            merge_entries.push_back(std::move(merge_entry));
            return true;
        }

        return handle_noop("Can't create merge queue node in ZooKeeper, because log was updated in every of {} tries", try_no);
    };

    std::unordered_set<String> target_pids;
    if (partition)
    {
        target_pids.insert(storage.getPartitionIDFromQuery(partition, query_context));
    }
    else
    {
        auto assignment = storage.replica_assignment->getAssignments(zookeeper, {}, /*force_refresh=*/true);
        for (const auto & [pid, _] : assignment)
            target_pids.insert(pid);

        auto local_parts = storage.getVisibleDataPartsVector(query_context);
        for (const auto & part : local_parts)
            target_pids.insert(part->info.getPartitionId());
    }

    auto assignments_per_pid = storage.replica_assignment->getAssignments(
        zookeeper, std::vector<String>(target_pids.begin(), target_pids.end()), /*force_refresh=*/true);

    std::unordered_set<String> local_pids;
    std::unordered_map<String, std::vector<String>> candidates_by_pid;

    for (const auto & pid : target_pids)
    {
        auto it = assignments_per_pid.find(pid);
        if (it == assignments_per_pid.end() || it->second.replicas.empty())
        {
            auto parts_lock = storage.readLockParts();
            if (storage.getAnyPartInPartition(pid, parts_lock))
                local_pids.insert(pid);
            else if (partition != nullptr)
                return handle_noop("Cannot select parts for optimization: partition {} has no assignment and no local parts", pid);
            else
                continue;
        }
        else
        {
            std::vector<String> assigned;
            for (const auto & rep : it->second.replicas)
                assigned.push_back(KeeperReplicaAssignment::stripCloningSuffix(rep));

            if (std::find(assigned.begin(), assigned.end(), storage.replica_name) != assigned.end())
            {
                local_pids.insert(pid);
            }
            else
            {
                std::sort(assigned.begin(), assigned.end()); // simple fixed order fallback
                candidates_by_pid[pid] = assigned;
            }
        }
    }

    auto assigned = std::make_shared<std::atomic<bool>>(false);
    for (const auto & pid : local_pids)
    {
        if (try_assign_merge(pid, partition != nullptr))
            (*assigned) = true;
    }

    std::unordered_map<String, std::vector<String>> pids_by_leader;
    std::unordered_map<String, std::vector<String>> fallback_candidates;

    for (const auto & [pid, ordered] : candidates_by_pid)
    {
        if (ordered.empty()) continue;
        pids_by_leader[ordered[0]].push_back(pid);
        fallback_candidates[pid] = std::vector<String>(ordered.begin() + 1, ordered.end());
    }

    if (!candidates_by_pid.empty())
        ProfileEvents::increment(ProfileEvents::SelectiveReplicationOptimizeForwardedCommands);

    auto max_distributed_connections = getMaxDistributedConnections(query_context);
    ThreadPool pool(CurrentMetrics::OptimizeForwardingThreads, CurrentMetrics::OptimizeForwardingThreadsActive, CurrentMetrics::OptimizeForwardingThreadsScheduled,
                    max_distributed_connections);

    std::mutex unreachable_pids_mutex;
    std::vector<String> unreachable_pids;

    /// Bug #2 fix: capture fallback_candidates by const reference instead
    /// of by value. pool.wait() below guarantees the map outlives all lambdas.
    for (const auto & [leader, pids] : pids_by_leader)
    {
        pool.scheduleOrThrowOnError([this, leader_replica = leader, target_pids_for_leader = pids, final, deduplicate, deduplicate_by_columns, cleanup, query_context, zookeeper, &fallback_candidates, assigned, &unreachable_pids_mutex, &unreachable_pids]() mutable
        {
            auto component_guard = Coordination::setCurrentComponent("SelectiveReplication::OptimizeForwarder::optimizeWithSelectiveRouting");
            for (const auto & pid : target_pids_for_leader)
            {
                bool success = false;
                String current_target = leader_replica;
                auto candidates = fallback_candidates[pid];

                while (!success)
                {
                    auto addrs = storage.selective_router->getReplicaAddresses({current_target}, zookeeper);
                    if (addrs.contains(current_target))
                    {
                        success = forwardOptimizeToReplica(current_target, pid, final, deduplicate, deduplicate_by_columns, cleanup, addrs[current_target], query_context);
                        if (success)
                            (*assigned) = true;
                    }

                    if (success || candidates.empty())
                        break;
                    current_target = candidates.front();
                    candidates.erase(candidates.begin());
                }

                if (!success)
                {
                    std::lock_guard lock(unreachable_pids_mutex);
                    unreachable_pids.push_back(pid);
                }
            }
        });
    }

    pool.wait();

    if (!unreachable_pids.empty())
    {
        if (!getSkipUnavailableShards(query_context))
        {
            throw Exception(ErrorCodes::ALL_REPLICAS_LOST,
                "Selective replication: all candidate replicas for partition(s) {} are unreachable",
                fmt::join(unreachable_pids, ", "));
        }
        LOG_WARNING(log, "Selective replication: skipped OPTIMIZE for partition(s) {} because all candidate replicas are unreachable and skip_unavailable_shards=1",
            fmt::join(unreachable_pids, ", "));
    }

    StorageReplicatedMergeTree::WatchEventByPath watch_events;
    for (auto & merge_entry : merge_entries)
        storage.waitForLogEntryToBeProcessedIfNecessary(merge_entry, query_context, watch_events);

    return *assigned;
}

}
}
