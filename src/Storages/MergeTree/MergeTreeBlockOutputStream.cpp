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
    std::cerr << "DEBUG write in!\n";
    storage.delayInsertOrThrowIfNeeded();

    std::cerr << "DEBUG before split!\n";
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block);
    for (auto & current_block : part_blocks)
    {
        std::cerr << "DEBUG before Stopwatch!\n";
        Stopwatch watch;

        std::cerr << "DEBUG before writeTempPart!\n";
        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block);
        std::cerr << "DEBUG before rename!\n";
        storage.renameTempPartAndAdd(part, &storage.increment);

        std::cerr << "DEBUG before addNewPart!\n";
        PartLog::addNewPart(storage.global_context, part, watch.elapsed());

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        std::cerr << "DEBUG before merging_mutating_task_handle!\n";
        if (storage.merging_mutating_task_handle)
            storage.merging_mutating_task_handle->signalReadyToRun();
    }
}

}
