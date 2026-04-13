#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>

#include <atomic>
#include <deque>

namespace DB
{

class MergeTreeReadChunkInfo;
struct MergeTreeReadTaskContext;
using MergeTreeReadTaskContextPtr = std::shared_ptr<MergeTreeReadTaskContext>;

/// Transform processor for the pipelined MergeTree reader.
/// Receives chunks from `MergeTreePrewhereSource` carrying `MergeTreeReadChunkInfo`,
/// reads the remaining (non-prewhere) columns via `continueReadingChain`,
/// and assembles the full result block.
class MergeTreeRestColumnsTransform : public ISimpleTransform
{
public:
    MergeTreeRestColumnsTransform(
        Block input_header_,
        Block output_header_,
        std::shared_ptr<std::atomic<size_t>> rest_bytes_counter_);

    String getName() const override { return "MergeTreeRestColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    using ReadResult = MergeTreeRangeReader::ReadResult;

    /// Detect task boundaries by comparing the task context pointer.
    /// On a new task, reset the range reader and copy patch mark ranges.
    void acquireReadersFromChunkInfo(const MergeTreeReadChunkInfo & chunk_info);

    /// Ensure `rest_range_reader` is initialized for the current rest reader.
    void ensureRangeReaderInitialized();

    /// Advance the rest reader past granules that were fully filtered by PREWHERE.
    void advancePastSkippedResults(MergeTreeReadChunkInfo & chunk_info);

    /// Read rest columns via `continueReadingChain`, fill virtuals/missing,
    /// and apply the PREWHERE filter.
    /// Sets `should_evaluate_missing_defaults` if defaults need evaluation.
    Columns readAndFilterRestColumns(ReadResult & read_result, size_t & num_read_rows, bool & should_evaluate_missing_defaults);

    /// Read patch data, apply patches at all three stages, perform alter conversions,
    /// and evaluate missing defaults.
    void applyPatchesAndConversions(
        ReadResult & read_result,
        const MergeTreeReadChunkInfo & chunk_info,
        Columns & rest_columns,
        bool should_evaluate_missing_defaults,
        const Chunk & chunk);

    /// Merge prewhere columns (from input chunk) and rest columns into the output chunk.
    void assembleOutputBlock(Chunk & chunk, Columns & rest_columns, size_t num_rows);

    /// Shared per-task context (rest reader, patches, sample block).
    /// Updated when a new task begins.
    MergeTreeReadTaskContextPtr current_task_context;

    /// Range reader wrapping the rest reader. Reset when the task changes.
    std::optional<MergeTreeRangeReader> rest_range_reader;

    ReadStepsPerformanceCounters read_steps_performance_counters;

    /// Shared counter: bytes read by this transform are accumulated here
    /// and drained by the upstream `MergeTreePrewhereSource` via `getReadProgress`.
    std::shared_ptr<std::atomic<size_t>> rest_bytes_counter;

    /// Per-transform mutable state for patch processing.
    /// These are populated from `current_task_context` on task boundaries.
    std::vector<std::deque<PatchReadResultPtr>> rest_patches_results;
    std::vector<MarkRanges> rest_patches_mark_ranges;

    LoggerPtr log = getLogger("MergeTreeRestColumnsTransform");
};

}
