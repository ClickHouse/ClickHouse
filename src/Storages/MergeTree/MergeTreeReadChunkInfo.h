#pragma once

#include <Processors/Chunk.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>

namespace DB
{

/// Per-task state shared across all chunks from the same read task.
/// Created by `MergeTreePrewhereSource` when a new task begins,
/// attached to every chunk via `MergeTreeReadChunkInfo`.
struct MergeTreeReadTaskContext
{
    /// Rest-column reader (the "main" reader from the task).
    MergeTreeReaderPtr rest_reader;

    /// Rest-column patch readers (filtered: only those with non-empty column lists).
    MergeTreePatchReaders rest_patches;

    /// Mark ranges for rest patch readers (one per patch part).
    std::vector<MarkRanges> patches_mark_ranges;

    /// Sample block from the prewhere chain. Needed by `RestColumnsTransform`
    /// so that `readPatches` receives a header matching `read_result->columns`.
    Block prewhere_sample_block;

    /// Task info (part, columns, alter conversions).
    MergeTreeReadTaskInfoPtr task_info;
};

using MergeTreeReadTaskContextPtr = std::shared_ptr<MergeTreeReadTaskContext>;

/// Carries MergeTree read state between pipelined processors.
/// `MergeTreePrewhereSource` produces this, `MergeTreeRestColumnsTransform` consumes it.
class MergeTreeReadChunkInfo : public ChunkInfoCloneable<MergeTreeReadChunkInfo>
{
public:
    MergeTreeReadChunkInfo() = default;
    MergeTreeReadChunkInfo(const MergeTreeReadChunkInfo &) = default;

    using ReadResult = MergeTreeRangeReader::ReadResult;

    /// Shared per-task state (rest reader, patches, sample block).
    /// Same pointer for all chunks from the same task.
    MergeTreeReadTaskContextPtr task_context;

    /// ReadResult from the prewhere step -- carries rows_per_granule, filter,
    /// started_ranges, and other metadata needed by `continueReadingChain`.
    std::shared_ptr<ReadResult> read_result;

    /// ReadResults from prewhere-filtered chunks (num_rows==0) that were skipped
    /// between the previous emitted chunk and this one. `RestColumnsTransform`
    /// must call `continueReadingChain` for each to advance its stream past
    /// the filtered granules, keeping it in sync with the prewhere reader.
    std::vector<std::shared_ptr<ReadResult>> skipped_read_results;

    /// Mark ranges remaining in the task after this chunk was read.
    MarkRanges remaining_mark_ranges;
};

}
