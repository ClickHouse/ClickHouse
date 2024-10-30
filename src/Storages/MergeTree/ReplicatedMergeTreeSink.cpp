#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Interpreters/PartLog.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEventsScope.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ThreadFuzzer.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <DataTypes/ObjectUtils.h>
#include <Core/Block.h>
#include <IO/Operators.h>
#include <fmt/core.h>


namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
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

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMilliseconds sleep_before_commit_local_part_in_replicated_table_ms;
}

namespace FailPoints
{
    extern const char replicated_merge_tree_commit_zk_fail_after_op[];
    extern const char replicated_merge_tree_insert_quorum_fail_0[];
    extern const char replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault[];
    extern const char replicated_merge_tree_insert_retry_pause[];
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

template<bool async_insert>
struct ReplicatedMergeTreeSinkImpl<async_insert>::DelayedChunk
{
    using BlockInfo = std::conditional_t<async_insert, AsyncInsertBlockInfo, SyncInsertBlockInfo>;
    struct Partition : public BlockInfo
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UInt64 elapsed_ns;
        ProfileEvents::Counters part_counters;

        Partition() = default;
        Partition(LoggerPtr log_,
                  MergeTreeDataWriter::TemporaryPart && temp_part_,
                  UInt64 elapsed_ns_,
                  BlockIDsType && block_id_,
                  BlockWithPartition && block_,
                  std::optional<BlockWithPartition> && unmerged_block_with_partition_,
                  ProfileEvents::Counters && part_counters_)
            : BlockInfo(log_, std::move(block_id_), std::move(block_), std::move(unmerged_block_with_partition_)),
              temp_part(std::move(temp_part_)),
              elapsed_ns(elapsed_ns_),
              part_counters(std::move(part_counters_))
        {}
    };

    DelayedChunk() = default;
    explicit DelayedChunk(size_t replicas_num_) : replicas_num(replicas_num_) {}

    size_t replicas_num = 0;

    std::vector<Partition> partitions;
};

std::vector<Int64> testSelfDeduplicate(std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes)
{
    MutableColumnPtr column = DataTypeInt64().createColumn();
    for (auto datum : data)
    {
        column->insert(datum);
    }
    Block block({ColumnWithTypeAndName(std::move(column), DataTypePtr(new DataTypeInt64()), "a")});
    std::vector<String> tokens(offsets.size());
    BlockWithPartition block1(std::move(block), Row(), std::move(offsets), std::move(tokens));
    ProfileEvents::Counters profile_counters;
    ReplicatedMergeTreeSinkImpl<true>::DelayedChunk::Partition part(
        getLogger("testSelfDeduplicate"), MergeTreeDataWriter::TemporaryPart(), 0, std::move(hashes), std::move(block1), std::nullopt, std::move(profile_counters));

    part.filterSelfDuplicate();

    ColumnPtr col = part.block_with_partition.block.getColumns()[0];
    std::vector<Int64> result;
    for (size_t i = 0; i < col->size(); i++)
    {
        result.push_back(col->getInt(i));
    }
    return result;
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

template<bool async_insert>
ReplicatedMergeTreeSinkImpl<async_insert>::ReplicatedMergeTreeSinkImpl(
    StorageReplicatedMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    size_t quorum_size,
    size_t quorum_timeout_ms_,
    size_t max_parts_per_block_,
    bool quorum_parallel_,
    bool deduplicate_,
    bool majority_quorum,
    ContextPtr context_,
    bool is_attach_,
    bool allow_attach_while_readonly_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , required_quorum_size(majority_quorum ? std::nullopt : std::make_optional<size_t>(quorum_size))
    , quorum_timeout_ms(quorum_timeout_ms_)
    , max_parts_per_block(max_parts_per_block_)
    , is_attach(is_attach_)
    , allow_attach_while_readonly(allow_attach_while_readonly_)
    , quorum_parallel(quorum_parallel_)
    , deduplicate(deduplicate_)
    , log(getLogger(storage.getLogName() + " (Replicated OutputStream)"))
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutData(metadata_snapshot, context_))
{
    /// The quorum value `1` has the same meaning as if it is disabled.
    if (required_quorum_size == 1)
        required_quorum_size = 0;
}

template<bool async_insert>
ReplicatedMergeTreeSinkImpl<async_insert>::~ReplicatedMergeTreeSinkImpl()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part.cancel();
    }

    delayed_chunk.reset();
}

template<bool async_insert>
size_t ReplicatedMergeTreeSinkImpl<async_insert>::checkQuorumPrecondition(const ZooKeeperWithFaultInjectionPtr & zookeeper)
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
         settings[Setting::insert_keeper_retry_max_backoff_ms]},
        context->getProcessListElement());
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

            quorum_info.status_path = storage.zookeeper_path + "/quorum/status";

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
            size_t quorum_size = getQuorumSize(replicas_number);

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

template<bool async_insert>
void ReplicatedMergeTreeSinkImpl<async_insert>::consume(Chunk & chunk)
{
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, false);

    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    const auto & settings = context->getSettingsRef();

    ZooKeeperWithFaultInjectionPtr zookeeper = ZooKeeperWithFaultInjection::createInstance(
        settings[Setting::insert_keeper_fault_injection_probability],
        settings[Setting::insert_keeper_fault_injection_seed],
        storage.getZooKeeper(),
        "ReplicatedMergeTreeSink::consume",
        log);

    /** If write is with quorum, then we check that the required number of replicas is now live,
      *  and also that for all previous parts for which quorum is required, this quorum is reached.
      * And also check that during the insertion, the replica was not reinitialized or disabled (by the value of `is_active` node).
      * TODO Too complex logic, you can do better.
      */
    size_t replicas_num = checkQuorumPrecondition(zookeeper);

    if (!storage_snapshot->object_columns.empty())
        convertDynamicColumnsToTuples(block, storage_snapshot);


    AsyncInsertInfoPtr async_insert_info;

    if constexpr (async_insert)
    {
        const auto async_insert_info_ptr = chunk.getChunkInfos().get<AsyncInsertInfo>();
        if (async_insert_info_ptr)
            async_insert_info = std::make_shared<AsyncInsertInfo>(async_insert_info_ptr->offsets, async_insert_info_ptr->tokens);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No chunk info for async inserts");
    }

    String block_dedup_token;
    auto token_info = chunk.getChunkInfos().get<DeduplicationToken::TokenInfo>();
    if (!token_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in ReplicatedMergeTreeSink for table: {}",
            storage.getStorageID().getNameForLogs());

    const bool need_to_define_dedup_token = !token_info->isDefined();

    if (token_info->isDefined())
        block_dedup_token = token_info->getToken();

    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context, async_insert_info);

    using DelayedPartition = typename ReplicatedMergeTreeSinkImpl<async_insert>::DelayedChunk::Partition;
    using DelayedPartitions = std::vector<DelayedPartition>;
    DelayedPartitions partitions;

    size_t streams = 0;
    bool support_parallel_write = false;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        ProfileEvents::Counters part_counters;
        auto profile_events_scope = std::make_unique<ProfileEventsScope>(&part_counters);

        /// Some merging algorithms can mofidy the block which loses the information about the async insert offsets
        /// when preprocessing or filtering data for async inserts deduplication we want to use the initial, unmerged block
        std::optional<BlockWithPartition> unmerged_block;

        if constexpr (async_insert)
        {
            /// we copy everything but offsets which we move because they are only used by async insert
            if (settings[Setting::optimize_on_insert] && storage.writer.getMergingMode() != MergeTreeData::MergingParams::Mode::Ordinary)
                unmerged_block.emplace(Block(current_block.block), Row(current_block.partition), std::move(current_block.offsets), std::move(current_block.tokens));
        }

        /// Write part to the filesystem under temporary name. Calculate a checksum.
        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        if (!support_parallel_write && temp_part.part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        BlockIDsType block_id;

        if constexpr (async_insert)
        {
            auto get_block_id = [&](BlockWithPartition & block_)
            {
                block_id = AsyncInsertBlockInfo::getHashesForBlocks(block_, temp_part.part->info.partition_id);
                LOG_TRACE(log, "async insert part, part id {}, block id {}, offsets {}, size {}", temp_part.part->info.partition_id, toString(block_id), toString(block_.offsets), block_.offsets.size());
            };
            get_block_id(unmerged_block ? *unmerged_block : current_block);
        }
        else
        {
            if (deduplicate)
            {
                /// We add the hash from the data and partition identifier to deduplication ID.
                /// That is, do not insert the same data to the same partition twice.
                block_id = temp_part.part->getZeroLevelPartBlockID(block_dedup_token);
                LOG_DEBUG(log, "Wrote block with ID '{}', {} rows{}", block_id, current_block.block.rows(), quorumLogMessage(replicas_num));
            }
            else
            {
                LOG_DEBUG(log, "Wrote block with {} rows{}", current_block.block.rows(), quorumLogMessage(replicas_num));
            }

            if (need_to_define_dedup_token)
            {
                chassert(temp_part.part);
                const auto hash_value = temp_part.part->getPartBlockIDHash();
                token_info->addChunkHash(toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]));
            }
        }

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
        streams += temp_part.streams.size();
        if (streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayedChunk(zookeeper);
            delayed_chunk = std::make_unique<ReplicatedMergeTreeSinkImpl<async_insert>::DelayedChunk>(replicas_num);
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk(zookeeper);

            streams = 0;
            support_parallel_write = false;
            partitions = DelayedPartitions{};
        }

        if constexpr (!async_insert)
        {
            /// Reset earlier to free memory.
            current_block.block.clear();
            current_block.partition.clear();
        }

        partitions.emplace_back(DelayedPartition(
            log,
            std::move(temp_part),
            elapsed_ns,
            std::move(block_id),
            std::move(current_block),
            std::move(unmerged_block),
            std::move(part_counters) /// profile_events_scope must be reset here.
        ));
    }

    if (need_to_define_dedup_token)
    {
        token_info->finishChunkHashes();
    }

    finishDelayedChunk(zookeeper);
    delayed_chunk = std::make_unique<ReplicatedMergeTreeSinkImpl::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);

    ++num_blocks_processed;
}

template<>
void ReplicatedMergeTreeSinkImpl<false>::finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        ProfileEventsScope scoped_attach(&partition.part_counters);

        partition.temp_part.finalize();

        auto & part = partition.temp_part.part;

        try
        {
            bool deduplicated = commitPart(zookeeper, part, partition.block_id, delayed_chunk->replicas_num).second;

            /// Set a special error code if the block is duplicate
            int error = (deduplicate && deduplicated) ? ErrorCodes::INSERT_WAS_DEDUPLICATED : 0;
            auto * mark_cache = storage.getContext()->getMarkCache().get();

            if (!error && mark_cache)
            {
                for (const auto & stream : partition.temp_part.streams)
                {
                    auto marks = stream.stream->releaseCachedMarks();
                    addMarksToCache(*part, marks, mark_cache);
                }
            }

            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), ExecutionStatus(error));
            StorageReplicatedMergeTree::incrementInsertedPartsProfileEvent(part->getType());
        }
        catch (...)
        {
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), ExecutionStatus::fromCurrentException("", true));
            throw;
        }
    }

    delayed_chunk.reset();
}

template<>
void ReplicatedMergeTreeSinkImpl<true>::finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        int retry_times = 0;
        /// users may have lots of same inserts. It will be helpful to deduplicate in advance.
        if (partition.filterSelfDuplicate())
        {
            LOG_TRACE(log, "found duplicated inserts in the block");
            partition.block_with_partition.partition = std::move(partition.temp_part.part->partition.value);
            partition.temp_part.cancel();
            partition.temp_part = storage.writer.writeTempPart(partition.block_with_partition, metadata_snapshot, context);
        }

        /// reset the cache version to zero for every partition write.
        /// Version zero allows to avoid wait on first iteration
        cache_version = 0;
        while (true)
        {
            partition.temp_part.finalize();
            auto conflict_block_ids = commitPart(zookeeper, partition.temp_part.part, partition.block_id, delayed_chunk->replicas_num).first;

            if (conflict_block_ids.empty())
            {
                if (auto * mark_cache = storage.getContext()->getMarkCache().get())
                {
                    for (const auto & stream : partition.temp_part.streams)
                    {
                        auto marks = stream.stream->releaseCachedMarks();
                        addMarksToCache(*partition.temp_part.part, marks, mark_cache);
                    }
                }

                auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
                PartLog::addNewPart(
                    storage.getContext(),
                    PartLog::PartLogEntry(partition.temp_part.part, partition.elapsed_ns, counters_snapshot),
                    ExecutionStatus(0));
                break;
            }

            storage.async_block_ids_cache.triggerCacheUpdate();
            ++retry_times;
            LOG_DEBUG(log, "Found duplicate block IDs: {}, retry times {}", toString(conflict_block_ids), retry_times);
            /// partition clean conflict
            partition.filterBlockDuplicate(conflict_block_ids, false);
            if (partition.block_with_partition.block.rows() == 0)
            {
                auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
                PartLog::addNewPart(
                    storage.getContext(),
                    PartLog::PartLogEntry(partition.temp_part.part, partition.elapsed_ns, counters_snapshot),
                    ExecutionStatus(ErrorCodes::INSERT_WAS_DEDUPLICATED));
                break;
            }
            partition.block_with_partition.partition = std::move(partition.temp_part.part->partition.value);
            /// partition.temp_part is already finalized, no need to call cancel
            partition.temp_part = storage.writer.writeTempPart(partition.block_with_partition, metadata_snapshot, context);
        }
    }

    delayed_chunk.reset();
}

template<>
bool ReplicatedMergeTreeSinkImpl<false>::writeExistingPart(MergeTreeData::MutableDataPartPtr & part)
{
    /// NOTE: No delay in this case. That's Ok.
    auto origin_zookeeper = storage.getZooKeeper();
    auto zookeeper = std::make_shared<ZooKeeperWithFaultInjection>(origin_zookeeper);

    size_t replicas_num = checkQuorumPrecondition(zookeeper);

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

    try
    {
        part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        String block_id = deduplicate ? fmt::format("{}_{}", part->info.partition_id, part->checksums.getTotalChecksumHex()) : "";
        bool deduplicated = commitPart(zookeeper, part, block_id, replicas_num).second;

        /// Set a special error code if the block is duplicate
        int error = (deduplicate && deduplicated) ? ErrorCodes::INSERT_WAS_DEDUPLICATED : 0;
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), ExecutionStatus(error));
        return deduplicated;
    }
    catch (...)
    {
        try_rollback_part_rename();
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), ExecutionStatus::fromCurrentException("", true));
        throw;
    }
}

template<bool async_insert>
std::vector<String> ReplicatedMergeTreeSinkImpl<async_insert>::detectConflictsInAsyncBlockIDs(const std::vector<String> & ids)
{
    auto conflict_block_ids = storage.async_block_ids_cache.detectConflicts(ids, cache_version);
    if (!conflict_block_ids.empty())
    {
        cache_version = 0;
    }
    return conflict_block_ids;
}

namespace
{

bool contains(const std::vector<String> & block_ids, const String & path)
{
    for (const auto & local_block_id : block_ids)
       if (local_block_id == path)
            return true;
    return false;
}

bool contains(const String & block_ids, const String & path)
{
    return block_ids == path;
}

String getBlockIdPath(const String & zookeeper_path, const String & block_id)
{
    if (!block_id.empty())
        return zookeeper_path + "/blocks/" + block_id;
    return String();
}

std::vector<String> getBlockIdPath(const String & zookeeper_path, const std::vector<String> & block_id)
{
    std::vector<String> result;
    result.reserve(block_id.size());
    for (const auto & single_block_id : block_id)
        result.push_back(zookeeper_path + "/async_blocks/" + single_block_id);
    return result;
}

}

struct CommitRetryContext
{
    enum Stages
    {
        LOCK_AND_COMMIT,
        DUPLICATED_PART,
        SUCCESS,
        ERROR
    };

    /// Possible ways:

    /// LOCK_AND_COMMIT -> DUPLICATED_PART
    /// LOCK_AND_COMMIT -> SUCCESS
    /// LOCK_AND_COMMIT -> ERROR

    /// DUPLICATED_PART -> SUCCESS
    /// DUPLICATED_PART -> ERROR

    Stages stage = LOCK_AND_COMMIT;

    String actual_part_name;
    std::vector<String> conflict_block_ids;
    bool part_was_deduplicated = false;
};

template<bool async_insert>
std::pair<std::vector<String>, bool> ReplicatedMergeTreeSinkImpl<async_insert>::commitPart(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    MergeTreeData::MutableDataPartPtr & part,
    const BlockIDsType & block_id,
    size_t replicas_num)
{
    /// It is possible that we alter a part with different types of source columns.
    /// In this case, if column was not altered, the result type will be different with what we have in metadata.
    /// For now, consider it is ok. See 02461_alter_update_respect_part_column_type_bug for an example.
    ///
    /// metadata_snapshot->check(part->getColumns());

    auto block_id_path = getBlockIdPath(storage.zookeeper_path, block_id);

    CommitRetryContext retry_context;

    const auto & settings = context->getSettingsRef();
    ZooKeeperRetriesControl retries_ctl(
        "commitPart",
        log,
        {settings[Setting::insert_keeper_max_retries],
         settings[Setting::insert_keeper_retry_initial_backoff_ms],
         settings[Setting::insert_keeper_retry_max_backoff_ms]},
        context->getProcessListElement());

    auto resolve_duplicate_stage = [&] () -> CommitRetryContext::Stages
    {
        if constexpr (async_insert)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Conflict block ids and block number lock should not "
                            "be empty at the same time for async inserts");
        }
        else
        {
            /// This block was already written to some replica. Get the part name for it.
            /// Note: race condition with DROP PARTITION operation is possible. User will get "No node" exception and it is Ok.
            retry_context.actual_part_name = zookeeper->get(block_id_path);

            bool exists_locally = bool(storage.getActiveContainingPart(retry_context.actual_part_name));

            if (exists_locally)
                ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);

            LOG_INFO(log, "Block with ID {} {} as part {}; ignoring it.",
                     block_id,
                     exists_locally ? "already exists locally" : "already exists on other replicas",
                     retry_context.actual_part_name);

            retry_context.part_was_deduplicated = true;

            return CommitRetryContext::SUCCESS;
        }
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
            quorum_entry.required_number_of_replicas = getQuorumSize(replicas_num);
            quorum_entry.replicas.insert(storage.replica_name);

            /** At this point, this node will contain information that the current replica received a part.
                * When other replicas will receive this part (in the usual way, processing the replication log),
                *  they will add themselves to the contents of this node.
                * When it contains information about `quorum` number of replicas, this node is deleted,
                *  which indicates that the quorum has been reached.
                */

            quorum_info.status_path = storage.zookeeper_path + "/quorum/status";
            if (quorum_parallel)
                quorum_info.status_path = storage.zookeeper_path + "/quorum/parallel/" + retry_context.actual_part_name;

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
        log_entry.quorum = getQuorumSize(replicas_num);
        log_entry.new_part_format = part->getFormat();

        if constexpr (!async_insert)
            log_entry.block_id = block_id;

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

        if constexpr (async_insert)
        {
            /// prefilter by cache
            retry_context.conflict_block_ids = detectConflictsInAsyncBlockIDs(block_id);
            if (!retry_context.conflict_block_ids.empty())
            {
                return CommitRetryContext::ERROR;
            }
        }

        /// Save the current temporary path in case we need to revert the change to retry (ZK connection loss)
        const String temporary_part_relative_path = part->getDataPartStorage().getPartDirectory();

        /// Obtain incremental block number and lock it. The lock holds our intention to add the block to the filesystem.
        /// We remove the lock just after renaming the part. In case of exception, block number will be marked as abandoned.
        /// Also, make deduplication check. If a duplicate is detected, no nodes are created.

        /// Allocate new block number and check for duplicates
        auto block_number_lock = storage.allocateBlockNumber(part->info.partition_id, zookeeper, block_id_path); /// 1 RTT

        ThreadFuzzer::maybeInjectSleep();

        if (!block_number_lock.has_value())
        {
            return CommitRetryContext::DUPLICATED_PART;
        }

        if constexpr (async_insert)
        {
            /// The truth is that we always get only one path from block_number_lock.
            /// This is a restriction of Keeper. Here I would like to use vector because
            /// I wanna keep extensibility for future optimization, for instance, using
            /// cache to resolve conflicts in advance.
            String conflict_path = block_number_lock->getConflictPath();
            if (!conflict_path.empty())
            {
                LOG_TRACE(log, "Cannot get lock, the conflict path is {}", conflict_path);
                retry_context.conflict_block_ids.push_back(conflict_path);

                return CommitRetryContext::ERROR;
            }
        }

        auto block_number = block_number_lock->getNumber();

        /// Set part attributes according to part_number.
        part->info.min_block = block_number;
        part->info.max_block = block_number;
        part->info.level = 0;
        part->info.mutation = 0;

        part->setName(part->getNewName(part->info));
        retry_context.actual_part_name = part->name;

        /// Prepare transaction to ZooKeeper
        /// It will simultaneously add information about the part to all the necessary places in ZooKeeper and remove block_number_lock.
        Coordination::Requests ops;

        get_logs_ops(ops);

        /// Deletes the information that the block number is used for writing.
        size_t block_unlock_op_idx = ops.size();
        block_number_lock->getUnlockOp(ops);

        get_quorum_ops(ops);

        size_t shared_lock_ops_id_begin = ops.size();
        storage.getLockSharedDataOps(*part, zookeeper, /*replace_zero_copy_lock*/ false, {}, ops);
        size_t shared_lock_op_id_end = ops.size();

        storage.getCommitPartOps(ops, part, block_id_path);

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

        Coordination::Responses responses;
        Coordination::Error multi_code = zookeeper->tryMultiNoThrow(ops, responses, /* check_session_valid */ true); /// 1 RTT

        if (multi_code == Coordination::Error::ZOK)
        {
            part->new_part_was_committed_to_zookeeper_after_rename_on_disk = true;
            sleep_before_commit_for_tests();
            transaction.commit();

            /// Lock nodes have been already deleted, do not delete them in destructor
            block_number_lock->assumeUnlocked();
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
                block_number_lock->assumeUnlocked();
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
            part->renameTo(temporary_part_relative_path, false);
            /// Throw an exception to set the proper keeper error and force a retry (if possible)
            zkutil::KeeperMultiException::check(multi_code, ops, responses);
        }

        transaction.rollback();

        if (!Coordination::isUserError(multi_code))
            throw Exception(
                    ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
                    "Unexpected ZooKeeper error while adding block {} with ID '{}': {}",
                    block_number,
                    toString(block_id),
                    multi_code);

        auto failed_op_idx = zkutil::getFailedOpIndex(multi_code, responses);
        String failed_op_path = ops[failed_op_idx]->getPath();

        if (multi_code == Coordination::Error::ZNODEEXISTS && !block_id_path.empty() && contains(block_id_path, failed_op_path))
        {
            /// Block with the same id have just appeared in table (or other replica), rollback the insertion.
            LOG_INFO(log, "Block with ID {} already exists (it was just appeared) for part {}. Ignore it.",
                     toString(block_id), part->name);

            if constexpr (async_insert)
            {
                retry_context.conflict_block_ids = std::vector<String>({failed_op_path});
                LOG_TRACE(log, "conflict when committing, the conflict block ids are {}",
                          toString(retry_context.conflict_block_ids));
                return CommitRetryContext::ERROR;
            }

            return CommitRetryContext::DUPLICATED_PART;
        }

        if (multi_code == Coordination::Error::ZNONODE && failed_op_idx == block_unlock_op_idx)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                            "Insert query (for block {}) was canceled by concurrent ALTER PARTITION or TRUNCATE",
                            block_number_lock->getPath());

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
                toString(block_id),
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
                case CommitRetryContext::DUPLICATED_PART:
                    retry_context.stage = resolve_duplicate_stage();
                    break;
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
                || retry_context.stage == CommitRetryContext::ERROR)
            {
                /// operation is done
                return;
            }
        }
    });

    if (!retry_context.conflict_block_ids.empty())
        return {retry_context.conflict_block_ids, false};

    if (retry_context.stage == CommitRetryContext::SUCCESS)
    {
        storage.merge_selecting_task->schedule();

        if (isQuorumEnabled())
        {
            quorum_info.status_path = storage.zookeeper_path + "/quorum/status";
            if (quorum_parallel)
                quorum_info.status_path = storage.zookeeper_path + "/quorum/parallel/" + retry_context.actual_part_name;

            ZooKeeperRetriesControl new_retry_controller = retries_ctl;
            new_retry_controller.actionAfterLastFailedRetry([&]
            {
                /// We do not know whether or not data has been inserted in other replicas
                new_retry_controller.setUserError(Exception(
                    ErrorCodes::UNKNOWN_STATUS_OF_INSERT,
                    "Unknown quorum status. The data was inserted in the local replica but we could not verify quorum. Reason: {}",
                    new_retry_controller.getLastKeeperErrorMessage()));
            });

            new_retry_controller.retryLoop([&]()
            {
                zookeeper->setKeeper(storage.getZooKeeper());
                waitForQuorum(
                    zookeeper,
                    retry_context.actual_part_name,
                    quorum_info.status_path,
                    quorum_info.is_active_node_version,
                    quorum_info.host_node_version,
                    replicas_num);
            });
        }
    }

    return {retry_context.conflict_block_ids, retry_context.part_was_deduplicated};
}

template<bool async_insert>
void ReplicatedMergeTreeSinkImpl<async_insert>::onStart()
{
    /// It's only allowed to throw "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, true);
}

template<bool async_insert>
void ReplicatedMergeTreeSinkImpl<async_insert>::onFinish()
{
    chassert(!isCancelled());

    const auto & settings = context->getSettingsRef();
    ZooKeeperWithFaultInjectionPtr zookeeper = ZooKeeperWithFaultInjection::createInstance(
        settings[Setting::insert_keeper_fault_injection_probability],
        settings[Setting::insert_keeper_fault_injection_seed],
        storage.getZooKeeper(),
        "ReplicatedMergeTreeSink::onFinish",
        log);

    finishDelayedChunk(zookeeper);
}

template<bool async_insert>
void ReplicatedMergeTreeSinkImpl<async_insert>::waitForQuorum(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const std::string & part_name,
    const std::string & quorum_path,
    int is_active_node_version,
    int host_node_version,
    size_t replicas_num) const
{
    /// We are waiting for quorum to be satisfied.
    LOG_TRACE(log, "Waiting for quorum '{}' for part {}{}", quorum_path, part_name, quorumLogMessage(replicas_num));

    fiu_do_on(FailPoints::replicated_merge_tree_insert_quorum_fail_0, { zookeeper->forceFailureBeforeOperation(); });

    while (true)
    {
        zkutil::EventPtr event = std::make_shared<Poco::Event>();

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

template<bool async_insert>
String ReplicatedMergeTreeSinkImpl<async_insert>::quorumLogMessage(size_t replicas_num) const
{
    if (!isQuorumEnabled())
        return "";
    return fmt::format(" (quorum {} of {} replicas)", getQuorumSize(replicas_num), replicas_num);
}

template<bool async_insert>
size_t ReplicatedMergeTreeSinkImpl<async_insert>::getQuorumSize(size_t replicas_num) const
{
    if (!isQuorumEnabled())
        return 0;

    if (required_quorum_size)
        return required_quorum_size.value();

    return replicas_num / 2 + 1;
}

template<bool async_insert>
bool ReplicatedMergeTreeSinkImpl<async_insert>::isQuorumEnabled() const
{
    return !required_quorum_size.has_value() || required_quorum_size.value() > 1;
}

template class ReplicatedMergeTreeSinkImpl<true>;
template class ReplicatedMergeTreeSinkImpl<false>;

}
