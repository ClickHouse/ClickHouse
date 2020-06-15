#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

Block MergeTreeBlockOutputStream::getHeader() const
{
    return storage.getSampleBlock();
}


void MergeTreeBlockOutputStream::write(const Block & block)
{
    storage.delayInsertOrThrowIfNeeded();

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block);
    MergeTreeData::DataPartsVector inserted_parts;
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block);
        storage.renameTempPartAndAdd(part, &storage.increment);
        inserted_parts.push_back(part);

        PartLog::addNewPart(storage.global_context, part, watch.elapsed());

        if (auto part_in_memory = asInMemoryPart(part))
        {
            storage.in_memory_merges_throttler.add(part_in_memory->block.bytes(), part_in_memory->rows_count);

            if (storage.merging_mutating_task_handle && !storage.in_memory_merges_throttler.needDelayMerge())
            {
                storage.in_memory_merges_throttler.reset();
                storage.merging_mutating_task_handle->wake();
            }
        }
        else if (storage.merging_mutating_task_handle)
        {
            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.merging_mutating_task_handle->wake();
        }
    }

    if (storage.getSettings()->in_memory_parts_insert_sync)
    {
        for (const auto & part : inserted_parts)
        {
            auto part_in_memory = asInMemoryPart(part);
            if (!part_in_memory->waitUntilMerged(in_memory_parts_timeout))
                throw Exception("Timeout exceeded while waiting to write part "
                    + part->name + " on disk", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }
}

}
