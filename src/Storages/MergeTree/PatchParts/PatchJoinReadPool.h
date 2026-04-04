#pragma once
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MarkRange.h>

#include <atomic>
#include <vector>

namespace DB
{

/// Lightweight read pool for reading patch parts via MergeTreeSelectProcessor.
/// Distributes mark ranges across threads using an atomic counter.
/// All tasks read from the same patch part, so readers are always reusable.
class PatchJoinReadPool : public IMergeTreeReadPool
{
public:
    PatchJoinReadPool(
        Block header_,
        MergeTreeReadTaskInfoPtr task_info_,
        MergeTreeReadTask::Extras extras_,
        std::vector<MarkRange> work_ranges_,
        MergeTreeReadTask::BlockSizeParams block_size_params_);

    String getName() const override { return "PatchJoinReadPool"; }
    Block getHeader() const override { return header; }
    bool preservesOrderOfRanges() const override { return false; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    Block header;
    MergeTreeReadTaskInfoPtr task_info;
    MergeTreeReadTask::Extras extras;
    std::vector<MarkRange> work_ranges;
    MergeTreeReadTask::BlockSizeParams block_size_params;
    std::atomic<size_t> next_item{0};
};

}
