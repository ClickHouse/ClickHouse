#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>

#include <atomic>
#include <deque>

namespace DB
{

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
    /// The main reader for the current task. Taken from ChunkInfo on first chunk of each task.
    MergeTreeReaderPtr rest_reader;

    /// Range reader wrapping rest_reader. Created when rest_reader changes.
    std::optional<MergeTreeRangeReader> rest_range_reader;

    ReadStepsPerformanceCounters read_steps_performance_counters;

    /// Shared counter: bytes read by this transform are accumulated here
    /// and drained by the upstream `MergeTreePrewhereSource` via `getReadProgress`.
    std::shared_ptr<std::atomic<size_t>> rest_bytes_counter;

    /// Rest-column patch readers and their state.
    MergeTreePatchReaders rest_patch_readers;
    std::vector<std::deque<PatchReadResultPtr>> rest_patches_results;
    std::vector<MarkRanges> rest_patches_mark_ranges;

    LoggerPtr log = getLogger("MergeTreeRestColumnsTransform");
};

}
