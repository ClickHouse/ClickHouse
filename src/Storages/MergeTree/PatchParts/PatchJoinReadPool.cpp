#include <Storages/MergeTree/PatchParts/PatchJoinReadPool.h>

namespace DB
{

PatchJoinReadPool::PatchJoinReadPool(
    Block header_,
    MergeTreeReadTaskInfoPtr task_info_,
    MergeTreeReadTask::Extras extras_,
    std::vector<MarkRange> work_ranges_,
    MergeTreeReadTask::BlockSizeParams block_size_params_)
    : header(std::move(header_))
    , task_info(std::move(task_info_))
    , extras(std::move(extras_))
    , work_ranges(std::move(work_ranges_))
    , block_size_params(block_size_params_)
{
}

MergeTreeReadTaskPtr PatchJoinReadPool::getTask(size_t /*task_idx*/, MergeTreeReadTask * previous_task)
{
    size_t idx = next_item.fetch_add(1);
    if (idx >= work_ranges.size())
        return nullptr;

    MarkRanges ranges = {work_ranges[idx]};
    std::vector<MarkRanges> patches_ranges;

    MergeTreeReadTask::Readers readers;
    if (previous_task)
    {
        /// Same patch part -- reuse readers to avoid recreating decompression state.
        readers = previous_task->releaseReaders();
        readers.updateAllMarkRanges(ranges);
    }
    else
    {
        readers = MergeTreeReadTask::createReaders(task_info, extras, ranges, patches_ranges);
    }

    return std::make_unique<MergeTreeReadTask>(
        task_info,
        std::move(readers),
        std::move(ranges),
        std::move(patches_ranges),
        block_size_params,
        /*size_predictor=*/ nullptr,
        /*updater=*/ nullptr);
}

}
