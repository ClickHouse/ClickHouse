#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/QueueModeColumns.h>
#include <Interpreters/PartLog.h>
#include <DataTypes/ObjectUtils.h>
#include <Common/ProfileEventsScope.h>

namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
}

namespace DB
{

struct CommittingBlockNumberTagger : private boost::noncopyable
{
    std::mutex & committing_block_numbers_mutex;
    std::set<Int64> & committing_block_numbers;
    Int64 block_number;

    CommittingBlockNumberTagger(std::mutex & committing_block_numbers_mutex_, std::set<Int64> & committing_block_numbers_, Int64 block_number_)
        : committing_block_numbers_mutex{committing_block_numbers_mutex_}
        , committing_block_numbers{committing_block_numbers_}
        , block_number{block_number_}
    {
    }

    ~CommittingBlockNumberTagger()
    {
        std::lock_guard guard(committing_block_numbers_mutex);
        size_t removed_count = committing_block_numbers.erase(block_number);
        chassert(removed_count == 1);
    }
};

struct MergeTreeSink::DelayedChunk
{
    struct Partition
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UInt64 elapsed_ns;
        String block_dedup_token;
        ProfileEvents::Counters part_counters;
        std::unique_ptr<CommittingBlockNumberTagger> committing_block_number_tagger;
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

void MergeTreeSink::consume(Chunk chunk)
{
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(nullptr, context, false);

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    if (!storage_snapshot->object_columns.empty())
        convertDynamicColumnsToTuples(block, storage_snapshot);

    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    using DelayedPartitions = std::vector<MergeTreeSink::DelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t streams = 0;
    bool support_parallel_write = false;

    for (auto & current_block : part_blocks)
    {
        std::unique_ptr<CommittingBlockNumberTagger> committing_block_number_tagger;

        if (storage.getSettings()->queue_mode)
        {
            auto partition_id = MergeTreePartition(current_block.partition).getID(metadata_snapshot->getPartitionKey().sample_block);

            {
                std::lock_guard guard(storage.committing_block_numbers_mutex);

                Int64 block_number = storage.increment.get();
                auto & partition_committing_block_numbers = storage.committing_block_numbers[partition_id];
                auto [_, inserted] = partition_committing_block_numbers.insert(block_number);
                chassert(inserted);

                committing_block_number_tagger = std::make_unique<CommittingBlockNumberTagger>(
                    storage.committing_block_numbers_mutex,
                    partition_committing_block_numbers,
                    block_number
                );
            }

            materializeSortingColumnsForQueueMode(current_block.block, partition_id, committing_block_number_tagger->block_number);
        }

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

        if (!support_parallel_write && temp_part.part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        String block_dedup_token;
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

        size_t max_insert_delayed_streams_for_parallel_write;

        if (settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_insert_delayed_streams_for_parallel_write = settings.max_insert_delayed_streams_for_parallel_write;
        else if (support_parallel_write)
            max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        else
            max_insert_delayed_streams_for_parallel_write = 0;

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
            .block_dedup_token = std::move(block_dedup_token),
            .part_counters = std::move(part_counters),
            .committing_block_number_tagger = std::move(committing_block_number_tagger),
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

    for (auto & partition : delayed_chunk->partitions)
    {
        ProfileEventsScope scoped_attach(&partition.part_counters);

        partition.temp_part.finalize();

        auto & part = partition.temp_part.part;

        std::optional<Int64> block_number;
        if (partition.committing_block_number_tagger)
            block_number = partition.committing_block_number_tagger->block_number;

        bool added = false;

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
        {
            auto lock = storage.lockParts();
            storage.fillNewPartName(part, lock, block_number);

            auto * deduplication_log = storage.getDeduplicationLog();
            if (deduplication_log)
            {
                const String block_id = part->getZeroLevelPartBlockID(partition.block_dedup_token);
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

        /// Explicitly drop committing block number after commit
        partition.committing_block_number_tagger.reset();

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
