#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <IO/Operators.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ThreadFuzzer.h>
#include <base/scope_guard.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <algorithm>
#include <vector>

namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
    extern const Event SelfDuplicatedAsyncInserts;
    extern const Event DuplicatedAsyncInserts;
    extern const Event DuplicationElapsedMicroseconds;

    extern const Event QuorumParts;
    extern const Event QuorumWaitMicroseconds;
    extern const Event QuorumFailedInserts;
}

namespace DB
{
namespace Setting
{
    extern const SettingsFloat insert_keeper_fault_injection_probability;
    extern const SettingsUInt64 insert_keeper_fault_injection_seed;
    extern const SettingsUInt64 insert_keeper_max_retries;
    extern const SettingsUInt64 insert_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 insert_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
    extern const SettingsBool optimize_on_insert;
}

namespace ServerSetting
{
    extern const ServerSettingsInsertDeduplicationVersions insert_deduplication_version;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMilliseconds sleep_before_commit_local_part_in_replicated_table_ms;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window_for_async_inserts;
}

namespace FailPoints
{
    extern const char replicated_merge_tree_commit_zk_fail_after_op[];
    extern const char replicated_merge_tree_insert_quorum_fail_0[];
    extern const char replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault[];
    extern const char replicated_merge_tree_insert_retry_pause[];
    extern const char rmt_delay_commit_part[];
}

namespace ErrorCodes
{
    extern const int TOO_FEW_LIVE_REPLICAS;
    extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int READONLY;
    extern const int UNKNOWN_STATUS_OF_INSERT;
    extern const int INSERT_WAS_DEDUPLICATED;
    extern const int DUPLICATE_DATA_PART;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_IS_READ_ONLY;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{
    /// Convert block id vector to string. Output at most 50 ids.
    template<typename T>
    inline String toString(const std::vector<T> & vec)
    {
        size_t size = vec.size();
        size = std::min<size_t>(size, 50);
        return fmt::format("({})", fmt::join(vec.begin(), vec.begin() + size, ","));
    }
}

ReplicatedMergeTreeSink::ReplicatedMergeTreeSink(
    bool async_insert_,
    StorageReplicatedMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    size_t quorum_size,
    size_t quorum_timeout_ms_,
    size_t max_parts_per_block_,
    bool quorum_parallel_,
    bool majority_quorum,
    ContextPtr context_,
    bool is_attach_,
    bool allow_attach_while_readonly_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , required_quorum_size(majority_quorum ? std::nullopt : std::make_optional<size_t>(quorum_size))
    , quorum_timeout_ms(quorum_timeout_ms_)
    , max_parts_per_block(max_parts_per_block_)
    , is_attach(is_attach_)
    , allow_attach_while_readonly(allow_attach_while_readonly_)
    , quorum_parallel(quorum_parallel_)
    , deduplicate(
        async_insert_
        ? (*storage.getSettings())[MergeTreeSetting::replicated_deduplication_window_for_async_inserts] != 0
        : (*storage.getSettings())[MergeTreeSetting::replicated_deduplication_window] != 0)
    , log(getLogger(storage.getLogName() + " (Replicated OutputStream)"))
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutData(metadata_snapshot, context_))
    , is_async_insert(async_insert_)
    , insert_deduplication_version(context->getServerSettings()[ServerSetting::insert_deduplication_version].value)
{
    /// The quorum value `1` has the same meaning as if it is disabled.
    if (required_quorum_size == 1)
        required_quorum_size = 0;

    if (is_async_insert && isQuorumEnabled() && !quorum_parallel)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Should be checked earlier: async inserts with quorum only make sense with enabled insert_quorum_parallel setting");

    LOG_DEBUG(log, "Create ReplicatedMergeTreeSink {} async_insert={}, deduplicate={}, quorum_size={}, max_parts_per_block={}, quorum_parallel={}, is_attach={}",
        storage.getStorageID().getNameForLogs(),
        is_async_insert,
        deduplicate,
        required_quorum_size.has_value() ? toString(*required_quorum_size) : "majority",
        max_parts_per_block_,
        quorum_parallel_,
        is_attach_);
}

ReplicatedMergeTreeSink::~ReplicatedMergeTreeSink()
{
    if (delayed_parts.empty())
        return;

    chassert(isCancelled() || std::uncaught_exceptions());

    for (auto & partition : delayed_parts)
    {
        partition.temp_part->cancel();
    }
    delayed_parts.clear();
}

size_t ReplicatedMergeTreeSink::checkQuorumPrecondition(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (!isQuorumEnabled())
        return 0;

    size_t replicas_number = 0;

    const auto & settings = context->getSettingsRef();
    ZooKeeperRetriesControl quorum_retries_ctl(
        "checkQuorumPrecondition",
        log,
        {settings[Setting::insert_keeper_max_retries],
         settings[Setting::insert_keeper_retry_initial_backoff_ms],
         settings[Setting::insert_keeper_retry_max_backoff_ms],
         context->getProcessListElement()});
    quorum_retries_ctl.retryLoop(
        [&]()
        {
            zookeeper->setKeeper(storage.getZooKeeper());

            /// Stop retries if in shutdown, note that we need to check
            /// shutdown_prepared_called, not shutdown_called, since the table
            /// will be marked as readonly after calling
            /// StorageReplicatedMergeTree::flushAndPrepareForShutdown(), and
            /// the final shutdown() can not be called if you have Buffer table
            /// that writes to this replicated table, until all the retries
            /// will be made.
            if (storage.is_readonly && storage.shutdown_prepared_called)
                throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to shutdown: replica_path={}", storage.replica_path);

            quorum_info.status_path = fs::path(storage.zookeeper_path) / "quorum" / "status";

            Strings replicas = zookeeper->getChildren(fs::path(storage.zookeeper_path) / "replicas");

            Strings exists_paths;
            exists_paths.reserve(replicas.size());
            for (const auto & replica : replicas)
                if (replica != storage.replica_name)
                    exists_paths.emplace_back(fs::path(storage.zookeeper_path) / "replicas" / replica / "is_active");

            auto exists_result = zookeeper->exists(exists_paths);
            auto get_results = zookeeper->get(Strings{storage.replica_path + "/is_active", storage.replica_path + "/host"});

            Coordination::Error keeper_error = Coordination::Error::ZOK;
            size_t active_replicas = 1; /// Assume current replica is active (will check below)
            for (size_t i = 0; i < exists_paths.size(); ++i)
            {
                auto error = exists_result[i].error;
                if (error == Coordination::Error::ZOK)
                    ++active_replicas;
                else if (Coordination::isHardwareError(error))
                    keeper_error = error;
            }

            replicas_number = replicas.size();
            size_t quorum_size = getQuorumSize();

            if (active_replicas < quorum_size)
            {
                if (Coordination::isHardwareError(keeper_error))
                    throw Coordination::Exception::fromMessage(keeper_error, "Failed to check number of alive replicas");

                throw Exception(
                    ErrorCodes::TOO_FEW_LIVE_REPLICAS,
                    "Number of alive replicas ({}) is less than requested quorum ({}/{}).",
                    active_replicas,
                    quorum_size,
                    replicas_number);
            }

            /** Is there a quorum for the last part for which a quorum is needed?
                * Write of all the parts with the included quorum is linearly ordered.
                * This means that at any time there can be only one part,
                *  for which you need, but not yet reach the quorum.
                * Information about this part will be located in `/quorum/status` node.
                * If the quorum is reached, then the node is deleted.
                */

            String quorum_status;
            if (!quorum_parallel && zookeeper->tryGet(quorum_info.status_path, quorum_status))
                throw Exception(
                    ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE,
                    "Quorum for previous write has not been satisfied yet. Status: {}",
                    quorum_status);

            /// Both checks are implicitly made also later (otherwise there would be a race condition).

            auto is_active = get_results[0];
            auto host = get_results[1];

            if (is_active.error == Coordination::Error::ZNONODE || host.error == Coordination::Error::ZNONODE)
                throw Exception(ErrorCodes::READONLY, "Replica is not active right now");

            quorum_info.is_active_node_version = is_active.stat.version;
            quorum_info.host_node_version = host.stat.version;
        });

    return replicas_number;
}

void ReplicatedMergeTreeSink::consume(Chunk & chunk)
{
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeSink::consume");
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, false);

    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    const auto & settings = context->getSettingsRef();

    ZooKeeperWithFaultInjectionPtr zookeeper = createKeeper("ReplicatedMergeTreeSink::consume");

    auto deduplication_info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();

    BlocksWithPartition part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    decltype(delayed_parts) current_parts;

    size_t total_streams = 0;
    bool support_parallel_write = false;

    std::vector<UInt128> all_partitions_block_ids;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        ProfileEvents::Counters part_counters;
        auto profile_events_scope = std::make_unique<ProfileEventsScope>(&part_counters);

        auto current_deduplication_info = deduplication_info->cloneSelf();

        {
            ProfileEventTimeIncrement<Microseconds> duplication_elapsed(ProfileEvents::DuplicationElapsedMicroseconds);

            auto result = current_deduplication_info->deduplicateSelf(deduplicate, current_block.partition_id, context);

            if (result.removed_rows > 0)
            {
                ProfileEvents::increment(ProfileEvents::SelfDuplicatedAsyncInserts, result.removed_tokens);
                LOG_DEBUG(
                    log,
                    "In partition {} self deduplication removed tokens {} out of {}, left rows {} in tokens {}, debug: {}",
                    current_block.partition_id,
                    result.removed_tokens,
                    current_deduplication_info->getCount(),
                    result.filtered_block->rows(),
                    result.deduplication_info->getCount(),
                    result.deduplication_info->debug());

                current_block.block = result.filtered_block;
                current_deduplication_info = result.deduplication_info;
            }
        }

        /// Write part to the filesystem under temporary name. Calculate a checksum.
        auto temp_part = writeNewTempPart(current_block);

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part->part)
            continue;

        if (!support_parallel_write && temp_part->part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        auto hash = temp_part->part->getPartBlockIDHash();
        current_deduplication_info->setPartWriterHashForPartition(hash, current_block.block->rows());

        LOG_DEBUG(
            log,
            "Wrote block with {} rows{} and deduplication blocks: {}, deduplication info: {}",
            current_block.block->rows(), quorumLogMessage(),
            fmt::join(getDeduplicationBlockIds(current_deduplication_info->getDeduplicationHashes(current_block.partition_id, deduplicate)), ", "),
            current_deduplication_info->debug());

        all_partitions_block_ids.push_back(hash);

        profile_events_scope.reset();
        UInt64 elapsed_ns = watch.elapsed();

        size_t max_insert_delayed_streams_for_parallel_write;
        if (settings[Setting::max_insert_delayed_streams_for_parallel_write].changed)
            max_insert_delayed_streams_for_parallel_write = settings[Setting::max_insert_delayed_streams_for_parallel_write];
        else if (support_parallel_write)
            max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        else
            max_insert_delayed_streams_for_parallel_write = 0;

        /// In case of too much columns/parts in block, flush explicitly.
        size_t current_streams = 0;
        for (const auto & stream : temp_part->streams)
            current_streams += stream.stream->getNumberOfOpenStreams();

        if (total_streams + current_streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayed(zookeeper);
            delayed_parts = std::move(current_parts);
            finishDelayed(zookeeper);

            total_streams = 0;
            support_parallel_write = false;
            current_parts.clear();
        }

        current_parts.push_back(
            {
                .log = log,
                .block_with_partition = std::move(current_block),
                .deduplication_info = std::move(current_deduplication_info),
                .temp_part = std::move(temp_part),
                .elapsed_ns = elapsed_ns,
                .part_counters = std::move(part_counters),
            });

        total_streams += current_streams;
    }

    deduplication_info->setPartWriterHashes(all_partitions_block_ids, chunk.getNumRows());

    finishDelayed(zookeeper);
    delayed_parts = std::move(current_parts);

    ++num_blocks_processed;
}

MergeTreeTemporaryPartPtr ReplicatedMergeTreeSink::writeNewTempPart(BlockWithPartition & block)
{
    return storage.writer.writeTempPart(block, metadata_snapshot, context);
}

void ReplicatedMergeTreeSink::finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (delayed_parts.empty())
        return;

    for (auto & partition : delayed_parts)
    {
        ExecutionStatus status;
        std::vector<std::string> block_ids_for_log;

        {
            Stopwatch watch;
            SCOPE_EXIT({
                partition.elapsed_ns += watch.elapsed();
            });
            auto profile_events_scope = std::make_unique<ProfileEventsScope>(&partition.part_counters);

            std::set<std::string> parts_to_wait_for_quorum;

            /// reset the cache version to zero for every partition write.
            /// Version zero allows to avoid wait on first iteration
            deduplication_async_inserts_cache_version = 0;
            size_t retry_times = 0;
            while (true)
            {
                partition.temp_part->finalize();
                auto deduplication_hashes = partition.deduplication_info->getDeduplicationHashes(partition.block_with_partition.partition_id, deduplicate);
                auto deduplication_blocks_ids = getDeduplicationBlockIds(deduplication_hashes);

                auto conflicts = commitPart(zookeeper, partition.temp_part->part, deduplication_hashes, deduplication_blocks_ids);

                if (isQuorumEnabled())
                {
                    if (conflicts.empty())
                        parts_to_wait_for_quorum.insert(partition.temp_part->part->name);
                    else
                    {
                        for (const auto & hash : conflicts)
                            if (hash.hasConflictPartName())
                                parts_to_wait_for_quorum.insert(hash.getConflictPartName());
                    }
                }

                if (conflicts.empty())
                {
                    // Successfully committed
                    block_ids_for_log = deduplication_blocks_ids;
                    partition.temp_part->prewarmCaches();
                    break;
                }

                ++retry_times;
                // TODO: sync debuplication could use cache too

                if (insert_deduplication_version != InsertDeduplicationVersions::OLD_SEPARATE_HASHES)
                    storage.deduplication_hashes_cache.triggerCacheUpdate();

                if (is_async_insert)
                    storage.async_block_ids_cache.triggerCacheUpdate();

                {
                    ProfileEventTimeIncrement<Microseconds> duplication_elapsed(ProfileEvents::DuplicationElapsedMicroseconds);

                    LOG_DEBUG(log, "Found duplicate block IDs: {}, retry times {}", fmt::join(getDeduplicationBlockIds(conflicts), ", "), retry_times);

                    auto result = partition.deduplication_info->deduplicateBlock(
                        getDeduplicationBlockIds(conflicts),
                        partition.block_with_partition.partition_id,
                        context);

                    if (is_async_insert)
                        ProfileEvents::increment(ProfileEvents::DuplicatedAsyncInserts, result.removed_tokens);
                    else
                        ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks, result.removed_tokens);

                    LOG_DEBUG(
                        log,
                        "After filtering by collision, removed rows {}/{}, removed tokets {}/{} from origin block, after retry remaining rows: {}, remaining tokens: {}, elapsed {} ms, new deduplication info debug: {}",
                        result.removed_rows,
                        partition.deduplication_info->getRows(),
                        result.removed_tokens,
                        partition.deduplication_info->getCount(),
                        result.filtered_block->rows(),
                        result.deduplication_info->getCount(),
                        duplication_elapsed.elapsed() / 1000,
                        result.deduplication_info->debug());

                    partition.block_with_partition.block = result.filtered_block;
                    partition.deduplication_info = std::move(result.deduplication_info);
                }

                if (partition.block_with_partition.block->rows() == 0)
                {
                    // Whole block was deduplicated

                    if (!is_async_insert)
                    {
                        chassert(conflicts.size() == 1);
                        auto block_id = conflicts.front().getBlockId();
                        auto actual_part_name = conflicts.front().getConflictPartName();
                        bool exists_locally = bool(storage.getActiveContainingPart(actual_part_name));
                        LOG_INFO(
                            log,
                            "Block with ID {} {} as part {}; ignoring it.",
                            block_id,
                            exists_locally ? "already exists locally" : "already exists on other replicas",
                            actual_part_name);
                    }

                    block_ids_for_log = getDeduplicationBlockIds(conflicts);
                    status = ExecutionStatus(ErrorCodes::INSERT_WAS_DEDUPLICATED, "The part was deduplicated");
                    break;
                }

                partition.block_with_partition.partition = MergeTreePartition(partition.temp_part->part->partition.value);
                /// partition.temp_part is already finalized, no need to call cancel
                partition.temp_part = writeNewTempPart(partition.block_with_partition);
            }

            // Do it before logging part to have correct elapsed time and profile events in PartLog
            if (isQuorumEnabled())
            {
                for (const auto & part : parts_to_wait_for_quorum)
                    resolveQuorum(zookeeper, part);
            }
        }

        // profile_events_scope has to be destroyed in the scope above
        auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
        PartLog::addNewPart(
            storage.getContext(),
            PartLog::PartLogEntry(partition.temp_part->part, partition.elapsed_ns, counters_snapshot),
            block_ids_for_log, // it is either blocks for committed part, or conflicting blocks for deduplicated part
            status);
    }

    delayed_parts.clear();
}

bool ReplicatedMergeTreeSink::writeExistingPart(MergeTreeData::MutableDataPartPtr & part)
{
    /// NOTE: No delay in this case. That's Ok.
    auto origin_zookeeper = storage.getZooKeeper();
    auto zookeeper = std::make_shared<ZooKeeperWithFaultInjection>(origin_zookeeper);

    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    String original_part_dir = part->getDataPartStorage().getPartDirectory();
    auto try_rollback_part_rename = [this, &part, &original_part_dir] ()
    {
        if (original_part_dir == part->getDataPartStorage().getPartDirectory())
            return;

        if (part->new_part_was_committed_to_zookeeper_after_rename_on_disk)
            return;

        /// Probably we have renamed the part on disk, but then failed to commit it to ZK.
        /// We should rename it back, otherwise it will be lost (e.g. if it was a part from detached/ and we failed to attach it).
        try
        {
            part->renameTo(original_part_dir, /*remove_new_dir_if_exists*/ false);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    };

    bool keep_non_zero_level = storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary;
    part->info.level = (keep_non_zero_level && part->info.level > 0) ? 1 : 0;
    part->info.mutation = 0;
    part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
    std::vector<DeduplicationHash> deduplication_hashes;
    if (deduplicate)
    {
        switch (insert_deduplication_version)
        {
            case InsertDeduplicationVersions::OLD_SEPARATE_HASHES:
                deduplication_hashes.emplace_back(DeduplicationHash::createSyncHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                break;
            case InsertDeduplicationVersions::COMPATIBLE_DOUBLE_HASHES:
                deduplication_hashes.emplace_back(DeduplicationHash::createSyncHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                deduplication_hashes.emplace_back(DeduplicationHash::createUnifiedHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                break;
            case InsertDeduplicationVersions::NEW_UNIFIED_HASHES:
                deduplication_hashes.emplace_back(DeduplicationHash::createUnifiedHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                break;
        }
    }

    auto deduplication_ids = getDeduplicationBlockIds(deduplication_hashes);

    try
    {
        auto conflicts = commitPart(zookeeper, part, deduplication_hashes, deduplication_ids);
        bool deduplicated = !conflicts.empty();

        int error = 0;
        String error_message;
        /// Set a special error code if the block is duplicate
        if (deduplicate && deduplicated)
        {
            error = ErrorCodes::INSERT_WAS_DEDUPLICATED;
            error_message = "The part was deduplicated";

            const auto & relative_path = part->getDataPartStorage().getRelativePath();
            const auto part_dir = fs::path(relative_path).parent_path().filename().string();

            if (relative_path.ends_with("detached/attaching_" + part->name + "/"))
            {
                /// Part came from ATTACH PART - rename back to detached/ (remove attaching_ prefix)
                fs::path new_relative_path = fs::path("detached") / part->getNewName(part->info);
                part->renameTo(new_relative_path, false);
            }
            else if (part_dir.starts_with("tmp_restore_" + part->name))
            {
                /// Part came from RESTORE with a temporary directory.
                /// Just remove the temporary part since it's a duplicate.
                LOG_DEBUG(log, "Removing deduplicated part {} from temporary path {}", part->name, relative_path);
                part->removeIfNeeded();
            }
            else
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected deduplicated part with relative path '{}' and part directory '{}'. "
                    "Expected relative path to end with 'detached/attaching_{}/' or part directory to start with 'tmp_restore_{}'.",
                    relative_path, part_dir, part->name, part->name);
            }
        }
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), deduplication_ids, ExecutionStatus(error, error_message));
        return deduplicated;
    }
    catch (...)
    {
        try_rollback_part_rename();
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), deduplication_ids, ExecutionStatus::fromCurrentException("", true));
        throw;
    }
}

std::vector<DeduplicationHash> ReplicatedMergeTreeSink::detectConflictsInAsyncBlockIDs(const std::vector<DeduplicationHash> & deduplication_hashes)
{
    if (insert_deduplication_version != InsertDeduplicationVersions::NEW_UNIFIED_HASHES)
    {
        auto conflict_block_ids = storage.async_block_ids_cache.detectConflicts(deduplication_hashes, deduplication_async_inserts_cache_version);
        if (!conflict_block_ids.empty())
        {
            deduplication_async_inserts_cache_version = 0;
            return conflict_block_ids;
        }
    }

    if (insert_deduplication_version != InsertDeduplicationVersions::OLD_SEPARATE_HASHES)
    {
        auto conflict_block_ids = storage.deduplication_hashes_cache.detectConflicts(deduplication_hashes, deduplication_cache_version);
        if (!conflict_block_ids.empty())
        {
            deduplication_cache_version = 0;
            return conflict_block_ids;
        }
    }

    return {};
}

struct CommitRetryContext
{
    enum Stages
    {
        LOCK_AND_COMMIT,
        RESOLVE_CONFLICTS,
        FILTER_CONFLICTS_AND_RETRY,
        SUCCESS,
        ERROR
    };

    /// Possible ways:

    /// LOCK_AND_COMMIT -> RESOLVE_CONFLICTS
    /// LOCK_AND_COMMIT -> FILTER_CONFLICTS_AND_RETRY
    /// LOCK_AND_COMMIT -> SUCCESS
    /// LOCK_AND_COMMIT -> ERROR

    /// RESOLVE_CONFLICTS -> FILTER_CONFLICTS_AND_RETRY
    /// RESOLVE_CONFLICTS -> ERROR

    Stages stage = LOCK_AND_COMMIT;

    String actual_part_name;
    std::vector<DeduplicationHash> conflict_deduplication_hashes;
};


std::vector<DeduplicationHash> ReplicatedMergeTreeSink::commitPart(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    MergeTreeData::MutableDataPartPtr & part,
    const std::vector<DeduplicationHash> & deduplication_hashes,
    const std::vector<String> & deduplication_block_ids)
{
    /// It is possible that we alter a part with different types of source columns.
    /// In this case, if column was not altered, the result type will be different with what we have in metadata.
    /// For now, consider it is ok. See 02461_alter_update_respect_part_column_type_bug for an example.
    ///
    /// metadata_snapshot->check(part->getColumns());

    CommitRetryContext retry_context;

    const auto & settings = context->getSettingsRef();
    ZooKeeperRetriesControl retries_ctl(
        "commitPart",
        log,
        {settings[Setting::insert_keeper_max_retries],
         settings[Setting::insert_keeper_retry_initial_backoff_ms],
         settings[Setting::insert_keeper_retry_max_backoff_ms],
         context->getProcessListElement()});

    auto resolve_duplicate_stage = [&] () -> CommitRetryContext::Stages
    {
        chassert(!retry_context.conflict_deduplication_hashes.empty());

        /// This block was already written to some replica. Get the part name for it.
        /// Note: race condition with DROP PARTITION operation is possible. User will get "No node" exception and it is Ok.
        auto response = zookeeper->tryGet(getDeduplicationPaths(storage.zookeeper_path, retry_context.conflict_deduplication_hashes));
        for (size_t i = 0; i < retry_context.conflict_deduplication_hashes.size(); ++i)
        {
            auto & deduplication_hash = retry_context.conflict_deduplication_hashes[i];
            const auto & resp = response[i];

            /// If we cannot get the node, then probably it was removed in the meantime. Just skip it then.
            if (resp.error == Coordination::Error::ZNONODE)
                continue;

            const String & part_name = resp.data;
            deduplication_hash.setConflictPartName(part_name);
        }

        return CommitRetryContext::FILTER_CONFLICTS_AND_RETRY;
    };

    auto get_quorum_ops = [&] (Coordination::Requests & ops)
    {
        /** If we need a quorum - create a node in which the quorum is monitored.
         * (If such a node already exists, then someone has managed to make another quorum record at the same time,
         *  but for it the quorum has not yet been reached.
         *  You can not do the next quorum record at this time.)
         */
        if (isQuorumEnabled())
        {
            ReplicatedMergeTreeQuorumEntry quorum_entry;
            quorum_entry.part_name = part->name;
            quorum_entry.required_number_of_replicas = getQuorumSize();
            quorum_entry.replicas.insert(storage.replica_name);

            /** At this point, this node will contain information that the current replica received a part.
                * When other replicas will receive this part (in the usual way, processing the replication log),
                *  they will add themselves to the contents of this node.
                * When it contains information about `quorum` number of replicas, this node is deleted,
                *  which indicates that the quorum has been reached.
                */

            quorum_info.status_path = fs::path(storage.zookeeper_path) / "quorum" / "status";
            if (quorum_parallel)
                quorum_info.status_path = fs::path(storage.zookeeper_path) / "quorum" / "parallel" / retry_context.actual_part_name;

            ops.emplace_back(
                    zkutil::makeCreateRequest(
                            quorum_info.status_path,
                            quorum_entry.toString(),
                            zkutil::CreateMode::Persistent));

            /// Make sure that during the insertion time, the replica was not reinitialized or disabled (when the server is finished).
            ops.emplace_back(
                    zkutil::makeCheckRequest(
                            storage.replica_path + "/is_active",
                            quorum_info.is_active_node_version));

            /// Unfortunately, just checking the above is not enough, because `is_active`
            /// node can be deleted and reappear with the same version.
            /// But then the `host` value will change. We will check this.
            /// It's great that these two nodes change in the same transaction (see MergeTreeRestartingThread).
            ops.emplace_back(
                    zkutil::makeCheckRequest(
                            storage.replica_path + "/host",
                            quorum_info.host_node_version));
        }
    };

    auto get_logs_ops = [&] (Coordination::Requests & ops)
    {
        ReplicatedMergeTreeLogEntryData log_entry;

        if (is_attach)
        {
            log_entry.type = ReplicatedMergeTreeLogEntry::ATTACH_PART;

            /// We don't need to involve ZooKeeper to obtain checksums as by the time we get
            /// MutableDataPartPtr here, we already have the data thus being able to
            /// calculate the checksums.
            log_entry.part_checksum = part->checksums.getTotalChecksumHex();
        }
        else
            log_entry.type = ReplicatedMergeTreeLogEntry::GET_PART;

        log_entry.create_time = time(nullptr);
        log_entry.source_replica = storage.replica_name;
        log_entry.new_part_name = part->name;
        /// TODO maybe add UUID here as well?
        log_entry.quorum = getQuorumSize();
        log_entry.new_part_format = part->getFormat();

        if (!is_async_insert && !deduplication_hashes.empty() && deduplicate)
        {
            chassert(deduplication_hashes.size() == 1 || insert_deduplication_version == InsertDeduplicationVersions::COMPATIBLE_DOUBLE_HASHES);
            log_entry.deduplication_block_ids = deduplication_block_ids;
        }

        /// Prepare an entry for log.
        ops.emplace_back(zkutil::makeCreateRequest(
                storage.zookeeper_path + "/log/log-",
                log_entry.toString(),
                zkutil::CreateMode::PersistentSequential));
    };

    auto sleep_before_commit_for_tests = [&] ()
    {
        auto sleep_before_commit_local_part_in_replicated_table_ms = (*storage.getSettings())[MergeTreeSetting::sleep_before_commit_local_part_in_replicated_table_ms];
        if (sleep_before_commit_local_part_in_replicated_table_ms.totalMilliseconds())
        {
            LOG_INFO(log, "committing part {}, triggered sleep_before_commit_local_part_in_replicated_table_ms {}",
                     part->name, sleep_before_commit_local_part_in_replicated_table_ms.totalMilliseconds());
            sleepForMilliseconds(sleep_before_commit_local_part_in_replicated_table_ms.totalMilliseconds());
        }
    };

    auto commit_new_part_stage = [&]() -> CommitRetryContext::Stages
    {
        if (storage.is_readonly)
        {
            /// stop retries if in shutdown
            if (storage.shutdown_prepared_called)
                throw Exception(
                    ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to shutdown: replica_path={}", storage.replica_path);

            /// Usually parts should not be attached in read-only mode. So we retry until the table is not read-only.
            /// However there is one case when it's necessary to attach in read-only mode - during execution of the RESTORE REPLICA command.
            if (!allow_attach_while_readonly)
            {
                retries_ctl.setUserError(
                    Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode: replica_path={}", storage.replica_path));
                return CommitRetryContext::LOCK_AND_COMMIT;
            }
        }

        if (is_async_insert)
        {
            /// prefilter by cache
            auto conflicts = detectConflictsInAsyncBlockIDs(deduplication_hashes);
            std::move(conflicts.begin(), conflicts.end(), std::back_inserter(retry_context.conflict_deduplication_hashes));

            if (!retry_context.conflict_deduplication_hashes.empty())
            {
                return CommitRetryContext::RESOLVE_CONFLICTS;
            }
        }

        /// Save the current temporary path and name in case we need to revert the change to retry (ZK connection loss) or in case part is deduplicated.
        const String temporary_part_relative_path = part->getDataPartStorage().getPartDirectory();
        const String initial_part_name = part->name;

        /// Obtain incremental block number and lock it. The lock holds our intention to add the block to the filesystem.
        /// We remove the lock just after renaming the part. In case of exception, block number will be marked as abandoned.
        /// Also, make deduplication check. If a duplicate is detected, no nodes are created.

        /// Allocate new block number and check for duplicates
        auto block_data = serializeCommittingBlockOpToString(CommittingBlock::Op::NewPart);
        auto block_id_pathes = getDeduplicationPaths(storage.zookeeper_path, deduplication_hashes);
        auto block_number_lock = storage.allocateBlockNumber(part->info.getPartitionId(), zookeeper, block_id_pathes, "", block_data); /// 1 RTT

        ThreadFuzzer::maybeInjectSleep();

        {
            std::filesystem::path conflict_path = block_number_lock.getConflictPath();
            if (!conflict_path.empty())
            {
                LOG_DEBUG(log, "Cannot get lock, the conflict path is {}", conflict_path);
                auto conflicted_hash_it = std::find_if(
                    deduplication_hashes.begin(),
                    deduplication_hashes.end(),
                    [&](const DeduplicationHash & hash)
                    {
                        return hash.getBlockId() == conflict_path.filename().string();
                    });
                chassert(conflicted_hash_it != deduplication_hashes.end());
                retry_context.conflict_deduplication_hashes.push_back(*conflicted_hash_it);

                return CommitRetryContext::RESOLVE_CONFLICTS;
            }
        }

        auto block_number = block_number_lock.getNumber();

        /// Set part attributes according to part_number.
        part->info.min_block = block_number;
        part->info.max_block = block_number;

        part->setName(part->getNewName(part->info));
        retry_context.actual_part_name = part->name;

        /// Prepare transaction to ZooKeeper
        /// It will simultaneously add information about the part to all the necessary places in ZooKeeper and remove block_number_lock.
        Coordination::Requests ops;

        get_logs_ops(ops);

        /// Deletes the information that the block number is used for writing.
        size_t block_unlock_op_idx = ops.size();
        block_number_lock.getUnlockOp(ops);

        get_quorum_ops(ops);

        size_t shared_lock_ops_id_begin = ops.size();
        storage.getLockSharedDataOps(*part, zookeeper, /*replace_zero_copy_lock*/ false, {}, ops);
        size_t shared_lock_op_id_end = ops.size();

        storage.getCommitPartOps(ops, part, block_id_pathes);

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(storage, NO_TRANSACTION_RAW); /// If you can not add a part to ZK, we'll remove it back from the working set.
        try
        {
            auto lock = storage.lockParts();
            storage.renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ true);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::DUPLICATE_DATA_PART || e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Part with name {} is already written by concurrent request."
                                " It should not happen for non-duplicate data parts because unique names are assigned for them. It's a bug",
                                part->name);
            }

            throw;
        }

        /// Rename parts before committing to ZooKeeper without holding DataPartsLock.
        transaction.renameParts();

        ThreadFuzzer::maybeInjectSleep();

        fiu_do_on(FailPoints::replicated_merge_tree_commit_zk_fail_after_op, { zookeeper->forceFailureAfterOperation(); });

        fiu_do_on(FailPoints::rmt_delay_commit_part, { sleepForSeconds(5); });

        Coordination::Responses responses;
        Coordination::Error multi_code = zookeeper->tryMultiNoThrow(ops, responses, /* check_session_valid */ true); /// 1 RTT
        if (multi_code == Coordination::Error::ZOK)
        {
            part->new_part_was_committed_to_zookeeper_after_rename_on_disk = true;
            sleep_before_commit_for_tests();
            transaction.commit();

            /// Lock nodes have been already deleted, do not delete them in destructor
            block_number_lock.assumeUnlocked();
            return CommitRetryContext::SUCCESS;
        }

        if (Coordination::isHardwareError(multi_code))
        {
            LOG_DEBUG(
                log, "Insert of part {} failed when committing to keeper (Reason: {}). Attempting to recover it", part->name, multi_code);
            ZooKeeperRetriesControl new_retry_controller = retries_ctl;

            /// We are going to try to verify if the transaction was written into keeper
            /// If we fail to do so (keeper unavailable) then we don't know if the changes were applied or not so
            /// we can't delete the local part, as if the changes were applied then inserted block appeared in
            /// `/blocks/`, and it can not be inserted again.
            new_retry_controller.actionAfterLastFailedRetry([&]
            {
                transaction.commit();
                storage.enqueuePartForCheck(part->name, MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER);
                throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_INSERT,
                        "Unknown status of part {} (Reason: {}). Data was written locally but we don't know the status in keeper. "
                        "The status will be verified automatically in ~{} seconds (the part will be kept if present in keeper or dropped if not)",
                        part->name, multi_code, MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER);
            });

            bool node_exists = false;
            bool quorum_fail_exists = false;
            /// The loop will be executed at least once
            new_retry_controller.retryLoop([&]
            {
                fiu_do_on(FailPoints::replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault, { zookeeper->forceFailureBeforeOperation(); });
                FailPointInjection::pauseFailPoint(FailPoints::replicated_merge_tree_insert_retry_pause);
                zookeeper->setKeeper(storage.getZooKeeper());
                node_exists = zookeeper->exists(fs::path(storage.replica_path) / "parts" / part->name);
                if (isQuorumEnabled())
                    quorum_fail_exists = zookeeper->exists(fs::path(storage.zookeeper_path) / "quorum" / "failed_parts" / part->name);
            });

            /// if it has quorum fail node, the restarting thread will clean the garbage.
            if (quorum_fail_exists)
            {
                LOG_INFO(log, "Part {} fails to commit and will not retry or clean garbage. Restarting Thread will do everything.", part->name);
                transaction.clear();
            /// `quorum/failed_parts/part_name` exists because table is read only for a while, So we return table is read only.
                throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to shutdown: replica_path={}", storage.replica_path);
            }

            if (node_exists)
            {
                LOG_DEBUG(log, "Insert of part {} recovered from keeper successfully. It will be committed", part->name);
                part->new_part_was_committed_to_zookeeper_after_rename_on_disk = true;
                sleep_before_commit_for_tests();
                transaction.commit();
                block_number_lock.assumeUnlocked();
                return CommitRetryContext::SUCCESS;
            }

            LOG_DEBUG(log, "Insert of part {} was not committed to keeper. Will try again with a new block", part->name);
            /// We checked in keeper and the the data in ops being written so we can retry the process again, but
            /// there is a caveat: as we lost the connection the block number that we got (EphemeralSequential)
            /// might or might not be there (and it belongs to a different session anyway) so we need to assume
            /// it's not there and will be removed automatically, and start from scratch
            /// In order to start from scratch we need to undo the changes that we've done as part of the
            /// transaction: renameTempPartAndAdd
            transaction.rollbackPartsToTemporaryState();
            part->is_temp = true;
            part->setName(initial_part_name);
            part->renameTo(temporary_part_relative_path, false);
            /// Throw an exception to set the proper keeper error and force a retry (if possible)
            zkutil::KeeperMultiException::check(multi_code, ops, responses);
        }

        auto failed_op_idx = zkutil::getFailedOpIndex(multi_code, responses);
        std::filesystem::path failed_op_path = ops[failed_op_idx]->getPath();

        if (multi_code == Coordination::Error::ZNODEEXISTS && !block_id_pathes.empty() && std::ranges::contains(block_id_pathes, failed_op_path.string()))
        {
            /// Block with the same id have just appeared in table (or other replica), rollback the insertion.
            LOG_INFO(log, "Block with ID {} already exists (it was just appeared) for part {}. Ignore it.",
                     failed_op_path, part->name);

            transaction.rollbackPartsToTemporaryState();
            part->is_temp = true;
            part->setName(initial_part_name);
            part->renameTo(temporary_part_relative_path, false);

            auto conflicted_hash_it = std::find_if(
                deduplication_hashes.begin(),
                deduplication_hashes.end(),
                [&](const DeduplicationHash & hash)
                {
                    return hash.getBlockId() == failed_op_path.filename().string();
                });
            chassert(conflicted_hash_it != deduplication_hashes.end());
            retry_context.conflict_deduplication_hashes.push_back(*conflicted_hash_it);

            LOG_TRACE(log, "conflict when committing, the conflict block ids are {}",
                fmt::join(getDeduplicationBlockIds(retry_context.conflict_deduplication_hashes), ", "));
            return CommitRetryContext::RESOLVE_CONFLICTS;
        }

        transaction.rollback();

        if (!Coordination::isUserError(multi_code))
            throw Exception(
                    ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
                    "Unexpected ZooKeeper error while adding block {} with ID '{}': {}",
                    block_number,
                    fmt::join(deduplication_block_ids, ", "),
                    multi_code);

        if (multi_code == Coordination::Error::ZNONODE && failed_op_idx == block_unlock_op_idx)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                            "Insert query (for block {}) was canceled by concurrent ALTER PARTITION or TRUNCATE",
                            block_number_lock.getPath());

        if (shared_lock_ops_id_begin <= failed_op_idx && failed_op_idx < shared_lock_op_id_end)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Creating shared lock for part {} has failed with error: {}. It's a bug. "
                            "No race is possible since it is a new part.",
                            part->name, multi_code);

        if (multi_code == Coordination::Error::ZNODEEXISTS && failed_op_path == quorum_info.status_path)
            throw Exception(ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE,
                            "Another quorum insert has been already started");

        throw Exception(
                ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
                "Unexpected logical error while adding block {} with ID '{}': {}, path {}",
                block_number,
                fmt::join(deduplication_block_ids, ", "),
                multi_code,
                failed_op_path);
    };

    auto stage_switcher = [&] ()
    {
        try
        {
            switch (retry_context.stage)
            {
                case CommitRetryContext::LOCK_AND_COMMIT:
                    retry_context.stage = commit_new_part_stage();
                    break;
                case CommitRetryContext::RESOLVE_CONFLICTS:
                    retry_context.stage = resolve_duplicate_stage();
                    break;
                case CommitRetryContext::FILTER_CONFLICTS_AND_RETRY:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Operation is already has a result.");
                case CommitRetryContext::SUCCESS:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Operation is already succeed.");
                case CommitRetryContext::ERROR:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Operation is already in error state.");
            }
        }
        catch (const zkutil::KeeperException &)
        {
            throw;
        }
        catch (DB::Exception &)
        {
            retry_context.stage = CommitRetryContext::ERROR;
            throw;
        }

        return retry_context.stage;
    };

    retries_ctl.retryLoop([&]()
    {
        zookeeper->setKeeper(storage.getZooKeeper());

        while (true)
        {
            const auto prev_stage = retry_context.stage;

            stage_switcher();

            if (prev_stage == retry_context.stage)
            {
                /// trigger next retry in retries_ctl.retryLoop when stage has not changed
                return;
            }

            if (retry_context.stage == CommitRetryContext::SUCCESS
                || retry_context.stage == CommitRetryContext::FILTER_CONFLICTS_AND_RETRY)
            {
                /// operation is done
                return;
            }
        }
    });

    if (retry_context.stage == CommitRetryContext::FILTER_CONFLICTS_AND_RETRY)
    {
        chassert(!retry_context.conflict_deduplication_hashes.empty());
    }

    if (retry_context.stage == CommitRetryContext::SUCCESS)
    {
        chassert(retry_context.conflict_deduplication_hashes.empty());
        storage.merge_selecting_task->schedule();
    }

    return retry_context.conflict_deduplication_hashes;
}

void ReplicatedMergeTreeSink::onStart()
{
    /// It's only allowed to throw "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, true);

    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeSink::onStart");
    ZooKeeperWithFaultInjectionPtr zookeeper = createKeeper("ReplicatedMergeTreeSink::onStart");
    /** If write is with quorum, then we check that the required number of replicas is now alive,
    *  and also that for all previous parts for which quorum is required, this quorum is reached.
    * And also check that during the insertion, the replica was not reinitialized or disabled (by the value of `is_active` node).
    * TODO Too complex logic, you can do better.
    */
    quorum_replicas_num = checkQuorumPrecondition(zookeeper);
}

void ReplicatedMergeTreeSink::onFinish()
{
    if (isCancelled())
        return;

    ZooKeeperWithFaultInjectionPtr zookeeper = createKeeper("ReplicatedMergeTreeSink::onFinish");
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeSink::onFinish");
    finishDelayed(zookeeper);
}

void ReplicatedMergeTreeSink::waitForQuorum(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const std::string & part_name,
    const std::string & quorum_path,
    int is_active_node_version,
    int host_node_version) const
{
    /// We are waiting for quorum to be satisfied.
    LOG_DEBUG(log, "Waiting for quorum '{}' for part {}{}", quorum_path, part_name, quorumLogMessage());

    fiu_do_on(FailPoints::replicated_merge_tree_insert_quorum_fail_0, { zookeeper->forceFailureBeforeOperation(); });

    while (true)
    {
        Coordination::EventPtr event = std::make_shared<Poco::Event>();

        std::string value;
        /// `get` instead of `exists` so that `watch` does not leak if the node is no longer there.
        if (!zookeeper->tryGet(quorum_path, value, nullptr, event))
            break;

        LOG_TRACE(log, "Quorum node {} still exists, will wait for updates", quorum_path);

        ReplicatedMergeTreeQuorumEntry quorum_entry(value);

        /// If the node has time to disappear, and then appear again for the next insert.
        if (quorum_entry.part_name != part_name)
            break;

        if (!event->tryWait(quorum_timeout_ms))
            throw Exception(
                ErrorCodes::UNKNOWN_STATUS_OF_INSERT,
                "Unknown quorum status. The data was inserted in the local replica but we could not verify quorum. Reason: "
                "Timeout while waiting for quorum");

        LOG_TRACE(log, "Quorum {} for part {} updated, will check quorum node still exists", quorum_path, part_name);
    }

    /// And what if it is possible that the current replica at this time has ceased to be active
    /// and the quorum is marked as failed and deleted?
    /// Note: checking is_active is not enough since it's ephemeral, and the version can be the same after recreation,
    /// so need to check host node as well
    auto get_results = zookeeper->tryGet(Strings{storage.replica_path + "/is_active", storage.replica_path + "/host"});
    const auto & is_active = get_results[0];
    const auto & host = get_results[1];
    if ((is_active.error == Coordination::Error::ZNONODE || is_active.stat.version != is_active_node_version)
        || (host.error == Coordination::Error::ZNONODE || host.stat.version != host_node_version))
        throw Exception(
            ErrorCodes::UNKNOWN_STATUS_OF_INSERT,
            "Unknown quorum status. The data was inserted in the local replica, but we could not verify the quorum. Reason: "
            "Replica became inactive while waiting for quorum");

    LOG_TRACE(log, "Quorum '{}' for part {} satisfied", quorum_path, part_name);
}

String ReplicatedMergeTreeSink::quorumLogMessage() const
{
    if (!isQuorumEnabled())
        return "";
    return fmt::format(" (quorum {} of {} replicas)", getQuorumSize(), quorum_replicas_num);
}

size_t ReplicatedMergeTreeSink::getQuorumSize() const
{
    if (!isQuorumEnabled())
        return 0;

    if (required_quorum_size)
        return required_quorum_size.value();

    return quorum_replicas_num / 2 + 1;
}

bool ReplicatedMergeTreeSink::isQuorumEnabled() const
{
    return !required_quorum_size.has_value() || required_quorum_size.value() > 1;
}

void ReplicatedMergeTreeSink::resolveQuorum(const ZooKeeperWithFaultInjectionPtr & zookeeper, std::string actual_part_name)
{
    if (actual_part_name.empty())
        return;

    if (!isQuorumEnabled())
        return;

    ProfileEvents::increment(ProfileEvents::QuorumParts);
    ProfileEventTimeIncrement<Microseconds> duplication_elapsed(ProfileEvents::QuorumWaitMicroseconds);

    quorum_info.status_path = fs::path(storage.zookeeper_path) / "quorum" / "status";
    if (quorum_parallel)
        quorum_info.status_path = fs::path(storage.zookeeper_path) / "quorum" / "parallel" / actual_part_name;

    const auto & settings = context->getSettingsRef();
    ZooKeeperRetriesControl quorum_retries_ctl(
        "waitQuorum",
        log,
        ZooKeeperRetriesInfo{
            settings[Setting::insert_keeper_max_retries],
            settings[Setting::insert_keeper_retry_initial_backoff_ms],
            settings[Setting::insert_keeper_retry_max_backoff_ms],
            context->getProcessListElement()
        });

    quorum_retries_ctl.actionAfterLastFailedRetry([&]
    {
        ProfileEvents::increment(ProfileEvents::QuorumFailedInserts);
        /// We do not know whether or not data has been inserted in other replicas
        quorum_retries_ctl.setUserError(
            Exception(
                ErrorCodes::UNKNOWN_STATUS_OF_INSERT,
                "Unknown quorum status. The data was inserted in the local replica but we could not verify quorum. Reason: {}",
                quorum_retries_ctl.getLastKeeperErrorMessage()));
    });

    quorum_retries_ctl.retryLoop([&]()
    {
        zookeeper->setKeeper(storage.getZooKeeper());
        waitForQuorum(
            zookeeper,
            actual_part_name,
            quorum_info.status_path,
            quorum_info.is_active_node_version,
            quorum_info.host_node_version);
    });
}

ZooKeeperWithFaultInjectionPtr ReplicatedMergeTreeSink::createKeeper(String name)
{
    const auto & settings = context->getSettingsRef();
    return ZooKeeperWithFaultInjection::createInstance(
        settings[Setting::insert_keeper_fault_injection_probability],
        settings[Setting::insert_keeper_fault_injection_seed],
        storage.getZooKeeper(),
        name,
        log);
}
}
