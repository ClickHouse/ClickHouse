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
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block);
        storage.renameTempPartAndAdd(part, &storage.increment);

        PartLog::addNewPart(storage.global_context, part, watch.elapsed());

        if (auto * part_in_memory = dynamic_cast<MergeTreeDataPartInMemory *>(part.get()))
        {
            storage.in_memory_merges_throttler.add(part_in_memory->block.bytes(), part_in_memory->rows_count);

            auto settings = storage.getSettings();
            if (settings->in_memory_parts_insert_sync)
            {
                if (!part->waitUntilMerged(in_memory_parts_timeout))
                    throw Exception("Timeout exceeded while waiting to write part "
                        + part->name + " on disk", ErrorCodes::TIMEOUT_EXCEEDED);
            }
            else if (storage.merging_mutating_task_handle && !storage.in_memory_merges_throttler.needDelayMerge())
            {
                storage.in_memory_merges_throttler.reset();
                storage.merging_mutating_task_handle->wake();
            }

            continue;
        }
        else if (storage.merging_mutating_task_handle)
        {
            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.merging_mutating_task_handle->wake();
        }
    }
}

}
