#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>


namespace DB
{

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
{
}

void MergeTreeSink::onStart()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded();
}

void MergeTreeSink::onFinish()
{
    finishDelayedChunk();
}

struct MergeTreeSink::DelayedChunk
{
    struct Partition
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UInt64 elapsed_ns;
        String block_dedup_token;
    };

    std::vector<Partition> partitions;
};


void MergeTreeSink::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto storage_snapshot = storage.getStorageSnapshot(metadata_snapshot, context);

    storage.writer.deduceTypesOfObjectColumns(storage_snapshot, block);
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);

    using DelayedPartitions = std::vector<MergeTreeSink::DelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t streams = 0;
    bool support_parallel_write = false;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;
        String block_dedup_token;

        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);

        UInt64 elapsed_ns = watch.elapsed();

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        if (!support_parallel_write && temp_part.part->volume->getDisk()->supportParallelWrite())
            support_parallel_write = true;

        if (storage.getDeduplicationLog())
        {
            const String & dedup_token = settings.insert_deduplication_token;
            if (!dedup_token.empty())
            {
                /// multiple blocks can be inserted within the same insert query
                /// an ordinal number is added to dedup token to generate a distinctive block id for each block
                block_dedup_token = fmt::format("{}_{}", dedup_token, chunk_dedup_seqnum);
                ++chunk_dedup_seqnum;
            }
        }

        size_t max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        if (!support_parallel_write || settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_insert_delayed_streams_for_parallel_write = settings.max_insert_delayed_streams_for_parallel_write;

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

        partitions.emplace_back(MergeTreeSink::DelayedChunk::Partition
        {
            .temp_part = std::move(temp_part),
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = std::move(block_dedup_token)
        });
    }

    finishDelayedChunk();
    delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);
}

void MergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part.finalize();

        auto & part = partition.temp_part.part;

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (storage.renameTempPartAndAdd(part, context->getCurrentTransaction().get(), &storage.increment, nullptr, storage.getDeduplicationLog(), partition.block_dedup_token))
        {
            PartLog::addNewPart(storage.getContext(), part, partition.elapsed_ns);

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger();
        }
    }

    delayed_chunk.reset();
}

}
