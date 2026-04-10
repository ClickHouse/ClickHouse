#pragma once

#include <Processors/Chunk.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>

namespace DB
{

/// Carries MergeTree read state between pipelined processors.
/// PrewhereSource produces this, RestColumnsTransform consumes it.
class MergeTreeReadChunkInfo : public ChunkInfoCloneable<MergeTreeReadChunkInfo>
{
public:
    MergeTreeReadChunkInfo() = default;

    /// Copy constructor needed by ChunkInfoCloneable::clone().
    /// rest_reader is move-only (unique_ptr), so clones get nullptr — ownership
    /// has already been transferred to RestColumnsTransform by the time clone is called.
    MergeTreeReadChunkInfo(const MergeTreeReadChunkInfo & other)
        : ChunkInfoCloneable<MergeTreeReadChunkInfo>(other)
        , read_result(other.read_result)
        , task_info(other.task_info)
        , rest_reader(nullptr)
        , remaining_mark_ranges(other.remaining_mark_ranges)
    {
    }

    using ReadResult = MergeTreeRangeReader::ReadResult;

    /// ReadResult from the prewhere step — carries rows_per_granule, filter,
    /// started_ranges, and other metadata needed by continueReadingChain.
    /// Stored as shared_ptr because ReadResult has private constructor.
    std::shared_ptr<ReadResult> read_result;

    /// Task info (part, columns, alter conversions). Shared across chunks from same task.
    MergeTreeReadTaskInfoPtr task_info;

    /// Rest-column reader. Set on first chunk of a new task, nullptr on subsequent chunks.
    /// RestColumnsTransform takes ownership on first use.
    MergeTreeReaderPtr rest_reader;

    /// Mark ranges remaining in the task after this chunk was read.
    MarkRanges remaining_mark_ranges;
};

}
