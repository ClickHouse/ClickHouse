#include <Storages/MergeTree/SelectiveReplication/InsertForwarder.h>
#include <Storages/MergeTree/SelectiveReplication/ForwardingUtils.h>
#include <Storages/MergeTree/SelectiveReplication/Router.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/InsertDeduplication.h>
#include <Core/Settings.h>
#include <base/defines.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/RemoteQueryCommon.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Sinks/RemoteSink.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Client/Connection.h>
#include <Client/ConnectionPool.h>
#include <IO/ConnectionTimeouts.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <fmt/core.h>
#include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event SelectiveReplicationInsertForwarded;
}

namespace CurrentMetrics
{
    extern const Metric DistributedInsertThreads;
    extern const Metric DistributedInsertThreadsActive;
    extern const Metric DistributedInsertThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ACTIVE_REPLICAS;
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int TOO_FEW_LIVE_REPLICAS;
}

namespace Setting
{
    extern const SettingsUInt64 distributed_connections_pool_size;
    extern const SettingsUInt64 max_distributed_connections;
    extern const SettingsFloat insert_keeper_fault_injection_probability;
    extern const SettingsUInt64 insert_keeper_fault_injection_seed;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 replication_factor;
}

namespace SelectiveReplication
{

InsertForwarder::InsertForwarder(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(getLogger(storage.getLogName() + " (SelectiveReplication::InsertForwarder)"))
{
}

bool InsertForwarder::isAssignedToSelf(const Strings & assigned) const
{
    return std::any_of(assigned.begin(), assigned.end(),
        [&](const String & r)
        {
            return KeeperReplicaAssignment::stripCloningSuffix(r) == storage.replica_name;
        });
}

InsertForwarder::ForwardingPlan InsertForwarder::buildForwardingPlan(
    BlocksWithPartition & blocks,
    const std::unordered_map<String, Strings> & assignments,
    const zkutil::ZooKeeperPtr & zk,
    const StorageMetadataPtr & /*metadata_snapshot*/)
{
    ForwardingPlan plan;

    std::unordered_map<String, Strings> partition_assigned_replicas;

    for (auto & current_block : blocks)
    {
        String lookup_id = current_block.partition_id;
        if (lookup_id.starts_with(MergeTreePartInfo::PATCH_PART_PREFIX))
            lookup_id = getOriginalPartitionIdOfPatch(lookup_id);

        auto it = assignments.find(lookup_id);
        if (it == assignments.end() || it->second.empty())
        {
            throw Exception(ErrorCodes::NO_ACTIVE_REPLICAS,
                "Selective replication: no replicas assigned for partition {} (original: {}). "
                "Check cluster health and rebalance state.",
                current_block.partition_id, lookup_id);
        }

        const Strings & assigned = it->second;
        bool self_assigned = isAssignedToSelf(assigned);
        if (self_assigned)
        {
            plan.local_blocks.push_back(std::move(current_block));
        }
        else
        {
            partition_assigned_replicas[current_block.partition_id] = assigned;
            plan.remote_blocks_storage.push_back(std::move(current_block));
        }
    }

    /// Group remote blocks by full sorted assignment vector to ensure exact fallback per partition.
    std::map<Strings, std::vector<size_t>> blocks_by_assignment;
    for (size_t idx = 0; idx < plan.remote_blocks_storage.size(); ++idx)
    {
        const auto & pid = plan.remote_blocks_storage[idx].partition_id;
        auto pa_it = partition_assigned_replicas.find(pid);
        chassert(pa_it != partition_assigned_replicas.end());
        blocks_by_assignment[pa_it->second].push_back(idx);
    }

    /// Convert to plan structures with serialized key.
    std::unordered_set<String> all_candidate_replicas;
    for (auto & [assigned, indices] : blocks_by_assignment)
    {
        String group_key = fmt::format("{}", fmt::join(assigned, ","));
        plan.blocks_by_replica[group_key] = std::move(indices);
        plan.replicas_to_try_map[group_key] = assigned;

        for (const auto & r : assigned)
            all_candidate_replicas.insert(r);
    }

    Strings candidate_list(all_candidate_replicas.begin(), all_candidate_replicas.end());
    plan.address_map = storage.selective_router->getReplicaAddresses(candidate_list, zk);

    return plan;
}

InsertForwarder::ForwardingPlan InsertForwarder::planForwardByAssignment(
    BlocksWithPartition & blocks,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context)
{
    auto zk = storage.getZooKeeper();
    auto all_replicas = storage.selective_router->getActiveReplicaNames(zk);
    const auto selective_settings = storage.getSettings();
    UInt64 rf = (*selective_settings)[MergeTreeSetting::replication_factor];

    std::vector<String> partition_ids;
    partition_ids.reserve(blocks.size());
    std::unordered_set<String> seen_partitions;
    for (const auto & current_block : blocks)
    {
        String lookup_id = current_block.partition_id;
        if (lookup_id.starts_with(MergeTreePartInfo::PATCH_PART_PREFIX))
            lookup_id = getOriginalPartitionIdOfPatch(lookup_id);
        if (seen_partitions.insert(lookup_id).second)
            partition_ids.push_back(lookup_id);
    }

    /// Batch-allocate all partitions in one call.
    auto batch_result = storage.replica_assignment->allocatePartitions(zk, partition_ids, all_replicas, rf);

    /// Merge assignments and existing into a single lookup map.
    std::unordered_map<String, Strings> all_assignments;
    for (auto & [pid, replicas] : batch_result.assignments)
        all_assignments[pid] = std::move(replicas);
    for (auto & [pid, replicas] : batch_result.existing)
        all_assignments[pid] = std::move(replicas);

    auto plan = buildForwardingPlan(blocks, all_assignments, zk, metadata_snapshot);
    plan.assignments = std::move(all_assignments);

    LOG_DEBUG(log, "Selective replication: phase 1 pre-routing at distributed_depth={}, "
                   "partitions={}, local_keep={}, forward_targets={}",
              context->getClientInfo().distributed_depth,
              blocks.size(),
              plan.local_blocks.size(),
              plan.blocks_by_replica.size());

    return plan;
}

InsertForwarder::ForwardingPlan InsertForwarder::planReForward(
    BlocksWithPartition & blocks,
    const zkutil::ZooKeeperPtr & zk,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr /*context*/)
{
    auto all_replicas = storage.selective_router->getActiveReplicaNames(zk);
    const auto selective_settings = storage.getSettings();
    UInt64 rf = (*selective_settings)[MergeTreeSetting::replication_factor];

    std::vector<String> partition_ids;
    partition_ids.reserve(blocks.size());
    std::unordered_set<String> seen_partitions;
    for (const auto & current_block : blocks)
    {
        String lookup_id = current_block.partition_id;
        if (lookup_id.starts_with(MergeTreePartInfo::PATCH_PART_PREFIX))
            lookup_id = getOriginalPartitionIdOfPatch(lookup_id);
        if (seen_partitions.insert(lookup_id).second)
            partition_ids.push_back(lookup_id);
    }

    auto batch_result = storage.replica_assignment->allocatePartitions(zk, partition_ids, all_replicas, rf);

    std::unordered_map<String, Strings> all_assignments;
    for (auto & [pid, replicas] : batch_result.assignments)
        all_assignments[pid] = std::move(replicas);
    for (auto & [pid, replicas] : batch_result.existing)
        all_assignments[pid] = std::move(replicas);

    auto plan = buildForwardingPlan(blocks, all_assignments, zk, metadata_snapshot);
    plan.assignments = std::move(all_assignments);

    return plan;
}

ConnectionPoolPtr InsertForwarder::createForwardingConnectionPool(
    const ReplicatedMergeTreeAddress & address,
    ContextPtr context) const
{
    return ForwardingUtils::createReplicaPool(
        address,
        storage.getContext(),
        static_cast<unsigned>(context->getSettingsRef()[Setting::distributed_connections_pool_size]));
}

void InsertForwarder::executeForwarding(
    const ForwardingPlan & plan,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const String & log_prefix)
{
    const auto & blocks_by_replica = plan.blocks_by_replica;
    const auto & remote_blocks_storage = plan.remote_blocks_storage;
    const auto & replicas_to_try_map = plan.replicas_to_try_map;
    const auto & address_map = plan.address_map;

    size_t num_targets = blocks_by_replica.size();
    if (num_targets == 0)
        return;

    std::vector<String> target_order;
    target_order.reserve(num_targets);
    for (const auto & [target, _] : blocks_by_replica)
        target_order.push_back(target);

    struct ForwardingResult
    {
        bool success = false;
        String error;
    };
    std::vector<ForwardingResult> forwarding_results(num_targets);

    auto do_forward = [&](size_t job_idx)
    {
        const String & target_replica = target_order[job_idx];
        const auto & fwd_block_indices = blocks_by_replica.at(target_replica);
        auto & fwd_result = forwarding_results[job_idx];

        const auto & replicas_to_try = replicas_to_try_map.at(target_replica);

        for (const auto & try_replica : replicas_to_try)
        {
            auto addr_it = address_map.find(try_replica);
            if (addr_it == address_map.end())
            {
                fwd_result.error = fmt::format("Replica {} host info not available in ZK", try_replica);
                LOG_DEBUG(log, "Selective replication {}: {}, trying next assigned replica", log_prefix, fwd_result.error);
                continue;
            }
            const auto & address = addr_it->second;

            try
            {
                /// Exclude MATERIALIZED columns from the INSERT query — they are computed
                /// by the receiving server and must not be explicitly inserted.
                Names physical_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
                const auto materialized_columns = metadata_snapshot->getColumns().getMaterialized();
                std::unordered_set<String> materialized_names;
                for (const auto & col : materialized_columns)
                    materialized_names.insert(col.name);

                Names insert_columns;
                insert_columns.reserve(physical_columns.size());
                for (const auto & col_name : physical_columns)
                {
                    if (!materialized_names.contains(col_name))
                        insert_columns.push_back(col_name);
                }

                String insert_query = createInsertToRemoteTableQuery(
                    address.database, address.table, insert_columns)->formatWithSecretsOneLine();

                auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(context->getSettingsRef());
                auto pool = createForwardingConnectionPool(address, context);

                auto remote_context = Context::createCopy(context);
                remote_context->increaseDistributedDepth();

                auto connection_entry = pool->get(timeouts, remote_context->getSettingsRef(), /*force_connected=*/true);

                auto remote_sink = std::make_shared<RemoteSink>(
                    *connection_entry, timeouts, insert_query,
                    remote_context->getSettingsRef(), remote_context->getClientInfo());
                QueryPipeline pipeline(std::move(remote_sink));
                PushingPipelineExecutor executor(pipeline);
                executor.start();

                for (size_t blk_idx : fwd_block_indices)
                {
                    /// Drop MATERIALIZED columns from the local block so its structure
                    /// matches the remote header (which only has non-MATERIALIZED columns).
                    /// Otherwise adoptBlock would emit a "Structure does not match" warning.
                    Block block_to_send = *remote_blocks_storage[blk_idx].block;
                    for (const auto & mat_col : materialized_names)
                    {
                        if (block_to_send.has(mat_col))
                            block_to_send.erase(mat_col);
                    }
                    Block adopted = adoptBlock(executor.getHeader(), block_to_send, log);
                    executor.push(adopted);
                }
                executor.finish();

                LOG_DEBUG(log, "Selective replication {}: forwarded {} partition(s) to replica {}",
                    log_prefix, fwd_block_indices.size(), try_replica);
                ProfileEvents::increment(ProfileEvents::SelectiveReplicationInsertForwarded, fwd_block_indices.size());
                fwd_result.success = true;
                return;
            }
            catch (const Exception &)
            {
                /// Only retry transport/connection-level failures (replica down, timeout, EOF).
                /// Application-level errors (e.g., permission denied, bad schema) must propagate.
                /// Reuses the community helper isRetryableException() used across MergeTree I/O paths.
                if (!isRetryableException(std::current_exception()))
                    throw;

                fwd_result.error = getCurrentExceptionMessage(/*with_stacktrace=*/false);
                LOG_DEBUG(log, "Selective replication {}: forwarding to replica {} failed with retryable error: {}. "
                               "Trying next assigned replica.",
                          log_prefix, try_replica, fwd_result.error);
                continue;
            }
        }
    };

    if (num_targets == 1)
    {
        do_forward(0);
    }
    else
    {
        /// Multiple targets — forward in parallel.
        size_t max_threads = std::min<size_t>(
            context->getSettingsRef()[Setting::max_distributed_connections], num_targets);
        ThreadPool forwarding_pool(
            CurrentMetrics::DistributedInsertThreads,
            CurrentMetrics::DistributedInsertThreadsActive,
            CurrentMetrics::DistributedInsertThreadsScheduled,
            max_threads, max_threads, num_targets);

        auto thread_group = CurrentThread::getGroup();
        for (size_t i = 0; i < num_targets; ++i)
        {
            forwarding_pool.scheduleOrThrowOnError([&do_forward, i, thread_group]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );
                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);

                setThreadName(ThreadName::SELECTIVE_REPLICATION_FORWARD);
                do_forward(i);
            });
        }

        forwarding_pool.wait();
    }

    for (size_t i = 0; i < num_targets; ++i)
    {
        if (!forwarding_results[i].success)
        {
            const String & target_replica = target_order[i];
            const auto & replicas_to_try = replicas_to_try_map.at(target_replica);
            throw Exception(ErrorCodes::ALL_REPLICAS_ARE_STALE,
                "Selective replication {}: all assigned replicas {} failed for {} partition(s). "
                "Last error: {}.",
                log_prefix,
                fmt::join(replicas_to_try, ", "),
                blocks_by_replica.at(target_replica).size(),
                forwarding_results[i].error);
        }
    }
}

void InsertForwarder::validateQuorumVsReplicationFactor(
    UInt64 quorum_size,
    UInt64 replication_factor)
{
    if (replication_factor > 0 && quorum_size > replication_factor)
        throw Exception(
            ErrorCodes::TOO_FEW_LIVE_REPLICAS,
            "insert_quorum ({}) cannot be greater than replication_factor ({}) "
            "in selective replication mode. Only the {} assigned replicas for each "
            "partition will confirm the write. Lower insert_quorum to at most {}.",
            quorum_size, replication_factor, replication_factor, replication_factor);
}

void InsertForwarder::validateQuorumForAssignments(
    const std::unordered_map<String, Strings> & assignments,
    const BlocksWithPartition & local_blocks,
    bool quorum_enabled,
    size_t quorum_size) const
{
    if (!quorum_enabled)
        return;

    for (const auto & block : local_blocks)
    {
        String pid = block.partition_id;
        if (pid.starts_with(MergeTreePartInfo::PATCH_PART_PREFIX))
            pid = getOriginalPartitionIdOfPatch(pid);

        auto it = assignments.find(pid);
        if (it == assignments.end() || it->second.empty())
            continue;

        if (it->second.size() < quorum_size)
            throw Exception(ErrorCodes::TOO_FEW_LIVE_REPLICAS,
                "Selective replication: partition {} has only {} assigned replica(s), "
                "but insert_quorum requires {}. Either reduce insert_quorum or wait "
                "for more replicas to become available.",
                pid, it->second.size(), quorum_size);
    }
}

int32_t InsertForwarder::getAssignmentCASVersion(
    const String & partition_id,
    const String & replica_name,
    const zkutil::ZooKeeperPtr & zk) const
{
    /// Fast path: check cache first
    auto cached_map = storage.replica_assignment->getAssignments(nullptr, {partition_id});
    auto cached_it = cached_map.find(partition_id);

    if (cached_it != cached_map.end()
        && KeeperReplicaAssignment::isReplicaAssigned(cached_it->second.replicas, replica_name))
        return cached_it->second.version;

    /// Cache miss, cold, or says not assigned -- verify with ZK
    auto result_map = storage.replica_assignment->getAssignments(zk, {partition_id}, /*force_refresh=*/true);
    auto result_it = result_map.find(partition_id);
    KeeperReplicaAssignment::CachedEntry result = result_it != result_map.end()
        ? result_it->second : KeeperReplicaAssignment::CachedEntry{};

    if (result.version < 0)
        throw AssignmentChangedException(
            partition_id,
            fmt::format(
                "Selective replication: partition {} has no assignment in ZK. "
                "Retry the INSERT or check replica availability.",
                partition_id));

    if (!KeeperReplicaAssignment::isReplicaAssigned(result.replicas, replica_name))
        throw AssignmentChangedException(
            partition_id,
            fmt::format("Selective replication: replica {} is no longer assigned for "
                "partition {}. Assignment may have changed. Retry the INSERT.",
                replica_name, partition_id));

    return result.version;
}

void InsertForwarder::addAssignmentCASCheck(
    Coordination::Requests & ops,
    const MergeTreeData::MutableDataPartPtr & part,
    const String & replica_name,
    const zkutil::ZooKeeperPtr & zk) const
{
    String partition_id = part->info.getPartitionId();
    if (part->info.isPatch())
        partition_id = part->info.getOriginalPartitionId();

    int32_t cas_version = getAssignmentCASVersion(partition_id, replica_name, zk);

    ops.emplace_back(zkutil::makeCheckRequest(
        storage.getZooKeeperPath() + "/selective/assignments/" + partition_id,
        cas_version));
}

InsertForwarder::CASMismatchAction InsertForwarder::handleCASVersionMismatch(
    const String & partition_id,
    const String & replica_name,
    LoggerPtr logger) const
{
    bool self_assigned = false;
    try
    {
        auto fresh_map = storage.replica_assignment->getAssignments(
            storage.getZooKeeper(), {partition_id}, /*force_refresh=*/true);
        auto fresh_it = fresh_map.find(partition_id);
        auto fresh = fresh_it != fresh_map.end()
            ? fresh_it->second : KeeperReplicaAssignment::CachedEntry{};
        self_assigned = KeeperReplicaAssignment::isReplicaAssigned(fresh.replicas, replica_name);
    }
    catch (const DB::Exception &)
    {
        tryLogCurrentException(logger, "Failed to read fresh assignment from ZK on ZBADVERSION");
        return CASMismatchAction::RETRY;
    }

    if (self_assigned)
    {
        LOG_DEBUG(logger, "Selective replication: assignment version changed for partition {} "
            "but replica {} is still assigned, retrying commit",
            partition_id, replica_name);
        return CASMismatchAction::RETRY;
    }

    return CASMismatchAction::REASSIGN;
}

InsertForwarder::CASFailureResult InsertForwarder::handleCommitCASFailure(
    MergeTreeData::MutableDataPartPtr & part,
    MergeTreeData::Transaction & transaction,
    const String & initial_part_name,
    const String & temporary_part_relative_path,
    const String & replica_name,
    LoggerPtr logger)
{
    String partition_id = part->info.getPartitionId();
    if (part->info.isPatch())
        partition_id = part->info.getOriginalPartitionId();

    auto action = handleCASVersionMismatch(partition_id, replica_name, logger);

    if (action == CASMismatchAction::RETRY)
    {
        transaction.rollbackPartsToTemporaryState();
        part->is_temp = true;
        part->setName(initial_part_name);
        part->renameTo(temporary_part_relative_path, false);
        return CASFailureResult::RETRY;
    }

    /// REASSIGN: no longer assigned
    transaction.rollback();
    return CASFailureResult::REASSIGN;
}

void InsertForwarder::handleAssignmentRace(
    std::vector<AssignmentFailure> & assignment_failures,
    const zkutil::ZooKeeperPtr & zk,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    std::function<MergeTreeTemporaryPartPtr(BlockWithPartition &)> write_temp_part,
    std::function<void(std::vector<DelayedPartInPartition> &, const ZooKeeperWithFaultInjectionPtr &, std::vector<AssignmentFailure> *)> finish_parts_fn,
    const DeduplicationInfo::Ptr & deduplication_info,
    LoggerPtr logger)
{
    if (context->getClientInfo().distributed_depth >= SelectiveReplication::MAX_FORWARDING_DEPTH)
    {
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH,
            "Selective replication: exceeded max forwarding depth ({}) "
            "while re-forwarding {} partition(s) with changed assignments. "
            "This usually indicates a routing loop; check `replica_assignment` "
            "consistency and cluster configuration.",
            SelectiveReplication::MAX_FORWARDING_DEPTH, assignment_failures.size());
    }

    LOG_INFO(logger, "Selective replication: phase 2 handling assignment race for {} partition(s) "
                 "at distributed_depth={}",
             assignment_failures.size(), context->getClientInfo().distributed_depth);

    const auto selective_settings = storage.getSettings();
    auto active_replicas = storage.selective_router->getActiveReplicaNames(zk);
    UInt64 rf = (*selective_settings)[MergeTreeSetting::replication_factor];

    /// Refresh assignments for failed partitions
    std::vector<String> partition_ids;
    partition_ids.reserve(assignment_failures.size());
    std::unordered_set<String> seen_partitions;
    for (const auto & failure : assignment_failures)
    {
        if (seen_partitions.insert(failure.partition_id).second)
            partition_ids.push_back(failure.partition_id);
    }

    storage.replica_assignment->allocatePartitions(zk, partition_ids, active_replicas, rf);

    /// Separate re-forward blocks
    BlocksWithPartition phase2_blocks;
    phase2_blocks.reserve(assignment_failures.size());
    for (auto & failure : assignment_failures)
        phase2_blocks.push_back(std::move(failure.block_with_partition));

    /// Use InsertForwarder for Phase 2 re-routing
    auto plan = planReForward(phase2_blocks, zk, metadata_snapshot, context);

    /// Write locally re-assigned blocks via the full commit loop
    if (!plan.local_blocks.empty())
    {
        std::vector<DelayedPartInPartition> phase2_local_parts;
        phase2_local_parts.reserve(plan.local_blocks.size());
        for (auto & local_block : plan.local_blocks)
        {
            Stopwatch phase2_watch;
            auto temp_part = write_temp_part(local_block);
            if (!temp_part->part)
                continue;

            phase2_local_parts.push_back(DelayedPartInPartition{
                .log = logger,
                .block_with_partition = std::move(local_block),
                .deduplication_info = deduplication_info->cloneSelf(),
                .temp_part = std::move(temp_part),
                .elapsed_ns = static_cast<UInt64>(phase2_watch.elapsed()),
                .part_counters = ProfileEvents::Counters(),
            });
        }

        auto zk_with_fi = ZooKeeperWithFaultInjection::createInstance(
            context->getSettingsRef()[Setting::insert_keeper_fault_injection_probability],
            context->getSettingsRef()[Setting::insert_keeper_fault_injection_seed],
            zk,
            "InsertForwarder::handleAssignmentRace",
            logger);
        finish_parts_fn(phase2_local_parts, zk_with_fi, nullptr);
    }

    /// Forward remote blocks
    if (!plan.remote_blocks_storage.empty())
        executeForwarding(plan, metadata_snapshot, context, "re-forward");
}

}
}
