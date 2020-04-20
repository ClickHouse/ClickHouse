#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
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

        if (isInMemoryPart(part) && storage.getSettings()->in_memory_parts_insert_sync)
        {
            if (!part->waitUntilMerged(in_memory_parts_timeout))
                throw Exception("Timeout exceeded while waiting to write part "
                    + part->name + " on disk", ErrorCodes::TIMEOUT_EXCEEDED);
        }

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        if (storage.merging_mutating_task_handle)
            storage.merging_mutating_task_handle->wake();
    }
}

}
