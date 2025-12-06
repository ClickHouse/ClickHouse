#pragma once
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

using RangesByIndex = std::unordered_map<size_t, RangesInDataPart>;
using ProjectionRangesByIndex = std::unordered_map<size_t, RangesInDataParts>;

class MergeTreeIndexReadResultPool;
using MergeTreeIndexReadResultPoolPtr = std::shared_ptr<MergeTreeIndexReadResultPool>;

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;
struct MergeTreeReadTaskInfo;

/// Read task for index.
/// Some indexes (e.g. inverted text index) may read special virtual columns.
struct IndexReadTask
{
    NamesAndTypesList columns;
    MergeTreeIndexWithCondition index;
    bool is_final;
};

using IndexReadTasks = std::unordered_map<String, IndexReadTask>;
using IndexReadColumns = std::unordered_map<String, NamesAndTypesList>;

/// A simple wrapper to allow atomic counters to be mutated even when accessed through a const map.
struct MutableAtomicSizeT
{
    mutable std::atomic_size_t value;
};

using PartRemainingMarks = std::unordered_map<size_t, MutableAtomicSizeT>;

/// Provides shared context needed to build filtering indexes (e.g., skip indexes or projection indexes) during data reads.
struct MergeTreeIndexBuildContext
{
    /// For each part, stores all ranges need to be read.
    const RangesByIndex read_ranges;

    /// For each part, stores a set of read ranges grouped by projection.
    const ProjectionRangesByIndex projection_read_ranges;

    /// Thread-safe shared pool for reading and building index filters
    const MergeTreeIndexReadResultPoolPtr index_reader_pool;

    /// Tracks how many marks are still being processed for each part during the execution phase. Once the count reaches
    /// zero for a part, its cached index can be released to free resources.
    const PartRemainingMarks part_remaining_marks;

    /// TODO: comment...
    const IndexReadTasks index_read_tasks;

    MergeTreeIndexBuildContext() = default;

    MergeTreeIndexBuildContext(
        RangesByIndex read_ranges_,
        ProjectionRangesByIndex projection_read_ranges_,
        MergeTreeIndexReadResultPoolPtr index_reader_pool_,
        PartRemainingMarks part_remaining_marks_,
        IndexReadTasks index_read_tasks_);

    MergeTreeIndexReadResultPtr getPreparedIndexReadResult(const MergeTreeReadTaskInfo & task_info, const MarkRanges & ranges) const;
};

using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

}
