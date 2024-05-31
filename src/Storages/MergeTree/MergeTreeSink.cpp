#include <Common/Exception.h>
#include <Common/ProfileEventsScope.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/PartLog.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>

#include <memory>

namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

struct MergeTreeSink::DelayedChunk
{
    struct Partition
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UInt64 elapsed_ns;
        String block_dedup_token;
        ProfileEvents::Counters part_counters;
    };

    std::vector<Partition> partitions;
};


MergeTreeSink::~MergeTreeSink() = default;

MergeTreeSink::MergeTreeSink(
    StorageMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    size_t max_parts_per_block_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutData(metadata_snapshot, context_))
{
    LOG_INFO(storage.log, "MergeTreeSink() called for {}.{}",
             storage_.getStorageID().database_name, storage_.getStorageID().getTableName());
}

void MergeTreeSink::onStart()
{
    /// It's only allowed to throw "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(nullptr, context, true);
}

void MergeTreeSink::onFinish()
{
    finishDelayedChunk();
}

void MergeTreeSink::consume(Chunk & chunk)
{
    LOG_INFO(storage.log, "consume() called num_blocks_processed {}, chunks: rows {} columns {} bytes {}",
             num_blocks_processed,
             chunk.getNumRows(), chunk.getNumColumns(), chunk.bytes());

    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(nullptr, context, false);

    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    if (!storage_snapshot->object_columns.empty())
        convertDynamicColumnsToTuples(block, storage_snapshot);

    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    LOG_INFO(storage.log, "consume() called part_blocks.count {}", part_blocks.size());

    using DelayedPartitions = std::vector<MergeTreeSink::DelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t streams = 0;
    bool support_parallel_write = false;

    String block_dedup_token;
    auto token_info = chunk.getChunkInfos().get<DeduplicationToken::TokenInfo>();
    if (storage.getDeduplicationLog())
    {
        if (!token_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "TokenInfo is expected for consumed chunk in MergeTreeSink for table: {}",
                storage.getStorageID().getNameForLogs());

        if (!token_info->tokenInitialized() && !context->getSettingsRef().insert_deduplication_token.value.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "TokenInfo has to be initialized with user token for table: {}, user dedup token {}",
                storage.getStorageID().getNameForLogs(),
                context->getSettingsRef().insert_deduplication_token.value);

        if (token_info->tokenInitialized())
        {
            block_dedup_token = token_info->getToken();

            LOG_DEBUG(storage.log,
                "dedup token from insert deduplication token in chunk: {}",
                block_dedup_token);
        }
        else
        {
            LOG_DEBUG(storage.log,
                "dedup token from hash is calculated");
        }
    }

    for (auto & current_block : part_blocks)
    {
        ProfileEvents::Counters part_counters;

        UInt64 elapsed_ns = 0;
        MergeTreeDataWriter::TemporaryPart temp_part;

        {
            ProfileEventsScope scoped_attach(&part_counters);

            Stopwatch watch;
            temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);
            elapsed_ns = watch.elapsed();
        }

        /// Reset earlier to free memory
        current_block.block.clear();
        current_block.partition.clear();

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        if (!token_info->tokenInitialized())
        {
            chassert(temp_part.part);
            token_info->setInitialToken(temp_part.part->getPartBlockIDHash());
        }

        if (!support_parallel_write && temp_part.part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        size_t max_insert_delayed_streams_for_parallel_write;

        if (settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_insert_delayed_streams_for_parallel_write = settings.max_insert_delayed_streams_for_parallel_write;
        else if (support_parallel_write)
            max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        else
            max_insert_delayed_streams_for_parallel_write = 0;

        LOG_INFO(storage.log, "consume() called for {}.{} "
                              "streams {} + {} -> {}, "
                              "max {} support_parallel_write {}",
                 storage.getStorageID().database_name, storage.getStorageID().getTableName(),
                 streams, temp_part.streams.size(), streams + temp_part.streams.size(),
                 max_insert_delayed_streams_for_parallel_write, support_parallel_write);

        /// In case of too much columns/parts in block, flush explicitly.
        streams += temp_part.streams.size();

        if (streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayedChunk();
            delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk();

            streams = 0;
            support_parallel_write = false;
            partitions = DelayedPartitions{};
        }

        /// TODO block_dedup_token
        partitions.emplace_back(MergeTreeSink::DelayedChunk::Partition
        {
            .temp_part = std::move(temp_part),
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = std::move(block_dedup_token),
            .part_counters = std::move(part_counters),
        });
    }

    finishDelayedChunk();
    delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);

    ++num_blocks_processed;
}

void MergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    LOG_INFO(storage.log, "finishDelayedChunk() called partitions count {}", delayed_chunk->partitions.size());

    for (auto & partition : delayed_chunk->partitions)
    {
        LOG_INFO(storage.log, "finishDelayedChunk() part name {} dedup_token {}", partition.temp_part.part->name, partition.block_dedup_token);

        ProfileEventsScope scoped_attach(&partition.part_counters);

        partition.temp_part.finalize();

        auto & part = partition.temp_part.part;

        bool added = false;

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
        {
            auto lock = storage.lockParts();
            storage.fillNewPartName(part, lock);

            auto * deduplication_log = storage.getDeduplicationLog();

            LOG_INFO(storage.log, "finishDelayedChunk() has dedup log {}", bool(deduplication_log));

            if (deduplication_log)
            {
                const String block_id = part->getZeroLevelPartBlockID(partition.block_dedup_token);

                LOG_INFO(storage.log, "finishDelayedChunk() block_dedup_token={}, block_id={}", partition.block_dedup_token, block_id);

                auto res = deduplication_log->addPart(block_id, part->info);
                if (!res.second)
                {
                    ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                    LOG_INFO(storage.log, "Block with ID {} already exists as part {}; ignoring it", block_id, res.first.getPartNameForLogs());
                    continue;
                }
            }

            added = storage.renameTempPartAndAdd(part, transaction, lock);
            transaction.commit(&lock);
        }

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (added)
        {
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot));
            StorageMergeTree::incrementInsertedPartsProfileEvent(part->getType());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger();
        }
    }

    delayed_chunk.reset();
}

}
