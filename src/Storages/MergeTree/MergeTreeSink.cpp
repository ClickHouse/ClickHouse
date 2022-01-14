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
    finishPrevPart();
}

struct MergeTreeSink::PrevPart
{
    MergeTreeDataWriter::TempPart temp_part;
    UInt64 elapsed_ns;
};


void MergeTreeSink::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);

        LOG_TRACE(&Poco::Logger::get("MergeTreeSink"), "Written part {}", temp_part.part->getNameWithState());

        UInt64 elapsed_ns = watch.elapsed();

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        finishPrevPart();

        prev_part = std::make_unique<MergeTreeSink::PrevPart>();
        prev_part->temp_part = std::move(temp_part);
        prev_part->elapsed_ns = elapsed_ns;
    }
}

void MergeTreeSink::finishPrevPart()
{
    if (prev_part)
    {

        LOG_TRACE(&Poco::Logger::get("MergeTreeSink"), "Finalizing  part {}", prev_part->temp_part.part->getNameWithState());
        prev_part->temp_part.finalize();
        LOG_TRACE(&Poco::Logger::get("MergeTreeSink"), "Finalized part {}", prev_part->temp_part.part->getNameWithState());

        auto & part = prev_part->temp_part.part;

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog()))
        {
            PartLog::addNewPart(storage.getContext(), part, prev_part->elapsed_ns);

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger();
        }

        LOG_TRACE(&Poco::Logger::get("MergeTreeSink"), "Renamed part {}", prev_part->temp_part.part->getNameWithState());
    }

    prev_part.reset();
}

}
