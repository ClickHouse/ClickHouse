#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>


namespace DB
{

Block MergeTreeBlockOutputStream::getHeader() const
{
    return storage.getSampleBlock();
}


void MergeTreeBlockOutputStream::write(const Block & block)
{
    storage.data.delayInsertOrThrowIfNeeded();

    auto part_blocks = storage.writer.splitBlockIntoParts(block);
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block);
        storage.data.renameTempPartAndAdd(part, &storage.increment);

        PartLog::addNewPart(storage.context, part, watch.elapsed());

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        storage.background_task_handle->wake();
    }
}

}
