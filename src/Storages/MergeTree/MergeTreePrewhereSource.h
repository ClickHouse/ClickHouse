#pragma once

#include <Processors/ISource.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>

namespace DB
{

struct PrewhereExprInfo;

/// Source processor for the pipelined MergeTree reader.
/// Reads prewhere columns, evaluates all PREWHERE steps (including mutation steps),
/// and outputs filtered chunks with `MergeTreeReadChunkInfo` attached.
/// The downstream `MergeTreeRestColumnsTransform` reads rest columns using
/// the `ReadResult` and main reader carried in the chunk info.
class MergeTreePrewhereSource : public ISource
{
public:
    MergeTreePrewhereSource(
        Block prewhere_header,
        MergeTreeReadPoolPtr pool_,
        size_t thread_idx_,
        PrewhereExprInfo prewhere_actions_,
        MergeTreeReadTask::BlockSizeParams block_size_params_);

    String getName() const override { return "MergeTreePrewhereSource"; }

    /// Computes the output header for the prewhere source by determining
    /// which columns from the result header belong to prewhere steps.
    static Block computePrewhereHeader(
        const Block & result_header,
        const Block & pool_header,
        const PrewhereExprInfo & prewhere_actions);

protected:
    std::optional<Chunk> tryGenerate() override;

private:
    MergeTreeReadPoolPtr pool;
    size_t thread_idx;
    PrewhereExprInfo prewhere_actions;
    MergeTreeReadTask::BlockSizeParams block_size_params;

    /// Per-task state. Rebuilt when switching to a new task.
    MergeTreeReadTaskInfoPtr current_task_info;
    MergeTreeReadTask::Readers current_readers;
    MergeTreeReadersChain prewhere_chain;
    MarkRanges mark_ranges;
    std::vector<MarkRanges> patches_mark_ranges;
    bool need_new_task = true;

    ReadStepsPerformanceCounters read_steps_performance_counters;
    LoggerPtr log = getLogger("MergeTreePrewhereSource");

    bool getNewTask();
};

}
