#pragma once

#include <Processors/ISource.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>

#include <atomic>

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
        size_t task_idx_,
        PrewhereExprInfo prewhere_actions_,
        MergeTreeReadTask::BlockSizeParams block_size_params_);

    String getName() const override { return "MergeTreePrewhereSource"; }

    /// Computes the output header for the prewhere source by determining
    /// which columns from the result header belong to prewhere steps.
    static Block computePrewhereHeader(
        const Block & result_header,
        const Block & pool_header,
        const PrewhereExprInfo & prewhere_actions);

    /// Shared counter for bytes read by the downstream `MergeTreeRestColumnsTransform`.
    /// The transform writes rest-column bytes here; this source drains them in
    /// `getReadProgress` so they are attributed to the read progress callback.
    std::shared_ptr<std::atomic<size_t>> getRestBytesCounter() { return rest_bytes_counter; }

    std::optional<ReadProgress> getReadProgress() override;

protected:
    std::optional<Chunk> tryGenerate() override;

private:
    MergeTreeReadPoolPtr pool;
    size_t task_idx;
    PrewhereExprInfo prewhere_actions;
    MergeTreeReadTask::BlockSizeParams block_size_params;

    /// Per-task state. Rebuilt when switching to a new task.
    MergeTreeReadTaskInfoPtr current_task_info;
    MergeTreeReadTask::Readers current_readers;
    MergeTreeReadersChain prewhere_chain;
    MarkRanges mark_ranges;
    std::vector<MarkRanges> patches_mark_ranges;
    /// Separate copy of patch mark ranges for the downstream RestColumnsTransform.
    /// The prewhere chain consumes `patches_mark_ranges` during read(), so the rest
    /// transform needs its own unconsumed copy to read rest-column patches independently.
    std::vector<MarkRanges> rest_patches_mark_ranges;
    bool need_new_task = true;

    /// Accumulated ReadResults from prewhere-filtered chunks (num_rows==0).
    /// Attached to the next non-empty chunk so `RestColumnsTransform` can
    /// advance its rest reader stream past the filtered granules.
    using ReadResultPtr = std::shared_ptr<MergeTreeRangeReader::ReadResult>;
    std::vector<ReadResultPtr> skipped_read_results;

    ReadStepsPerformanceCounters read_steps_performance_counters;

    /// Bytes read by the downstream RestColumnsTransform, accumulated atomically.
    std::shared_ptr<std::atomic<size_t>> rest_bytes_counter = std::make_shared<std::atomic<size_t>>(0);

    LoggerPtr log = getLogger("MergeTreePrewhereSource");

    bool getNewTask();
};

}
