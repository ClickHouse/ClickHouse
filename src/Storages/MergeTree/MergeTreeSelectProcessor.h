#pragma once

#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Processors/Chunk.h>

namespace DB
{

struct PrewhereExprInfo;

struct LazilyReadInfo;
using LazilyReadInfoPtr = std::shared_ptr<LazilyReadInfo>;

struct ChunkAndProgress
{
    Chunk chunk;
    size_t num_read_rows = 0;
    size_t num_read_bytes = 0;
    /// Explicitly indicate that we have read all data.
    /// This is needed to occasionally return empty chunk to indicate the progress while the rows are filtered out in PREWHERE.
    bool is_finished = false;
};

class ParallelReadingExtension
{
public:
    ParallelReadingExtension(
        MergeTreeAllRangesCallback all_callback_,
        MergeTreeReadTaskCallback callback_,
        size_t number_of_current_replica_,
        size_t total_nodes_count_);

    void sendInitialRequest(CoordinationMode mode, const RangesInDataParts & ranges, size_t mark_segment_size) const;

    std::optional<ParallelReadResponse>
    sendReadRequest(CoordinationMode mode, size_t min_number_of_marks, const RangesInDataPartsDescription & description) const;

    size_t getTotalNodesCount() const { return total_nodes_count; }
    size_t getNumberOfCurrentReplica() const { return number_of_current_replica; }
    MergeTreeAllRangesCallback getAllRangesCallback() const { return all_callback; }
    MergeTreeReadTaskCallback getReadTaskCallback() const { return callback; }

private:
    MergeTreeAllRangesCallback all_callback;
    MergeTreeReadTaskCallback callback;
    const size_t number_of_current_replica;
    const size_t total_nodes_count;
};

using RangesByIndex = std::unordered_map<size_t, RangesInDataPart>;
using ProjectionRangesByIndex = std::unordered_map<size_t, RangesInDataParts>;
class MergeTreeIndexReadResultPool;
using MergeTreeIndexReadResultPoolPtr = std::shared_ptr<MergeTreeIndexReadResultPool>;

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

    /// Thread-safe shared pool for reading and building index filters. Must not be null (enforced in constructor).
    const MergeTreeIndexReadResultPoolPtr index_reader_pool;

    /// Tracks how many marks are still being processed for each part during the execution phase. Once the count reaches
    /// zero for a part, its cached index can be released to free resources.
    const PartRemainingMarks part_remaining_marks;

    MergeTreeIndexBuildContext(
        RangesByIndex read_ranges_,
        ProjectionRangesByIndex projection_read_ranges_,
        MergeTreeIndexReadResultPoolPtr index_reader_pool_,
        PartRemainingMarks part_remaining_marks_);

    MergeTreeIndexReadResultPtr getPreparedIndexReadResult(const MergeTreeReadTask & task) const;
};

using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

/// Base class for MergeTreeThreadSelectAlgorithm and MergeTreeSelectAlgorithm
class MergeTreeSelectProcessor : private boost::noncopyable
{
public:
    MergeTreeSelectProcessor(
        MergeTreeReadPoolPtr pool_,
        MergeTreeSelectAlgorithmPtr algorithm_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const IndexReadTasks & index_read_tasks_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        MergeTreeIndexBuildContextPtr merge_tree_index_build_context_ = {});

    String getName() const;

    static Block transformHeader(
        Block block,
        const LazilyReadInfoPtr & lazily_read_info,
        const FilterDAGInfoPtr & row_level_filter,
        const PrewhereInfoPtr & prewhere_info);

    Block getHeader() const { return result_header; }

    ChunkAndProgress read();

    void cancel() noexcept;

    const MergeTreeReaderSettings & getSettings() const { return reader_settings; }

    static PrewhereExprInfo getPrewhereActions(
        const FilterDAGInfoPtr & row_level_filter,
        const PrewhereInfoPtr & prewhere_info,
        const IndexReadTasks & index_read_tasks,
        const ExpressionActionsSettings & actions_settings,
        bool enable_multiple_prewhere_read_steps,
        bool force_short_circuit_execution);

    void addPartLevelToChunk(bool add_part_level_) { add_part_level = add_part_level_; }

    void onFinish() const;

private:
    friend class SingleProjectionIndexReader;

    static void injectLazilyReadColumns(size_t rows, Block & block, size_t part_index, const LazilyReadInfoPtr & lazily_read_info);

    /// Sets up range readers corresponding to data readers
    void initializeReadersChain();

    const MergeTreeReadPoolPtr pool;
    const MergeTreeSelectAlgorithmPtr algorithm;

    const FilterDAGInfoPtr row_level_filter;
    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const PrewhereExprInfo prewhere_actions;

    const LazilyReadInfoPtr lazily_read_info;

    const MergeTreeReaderSettings reader_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;

    /// Current task to read from.
    MergeTreeReadTaskPtr task;
    /// A result of getHeader(). A chunk which this header is returned from read().
    Block result_header;

    ReadStepsPerformanceCounters read_steps_performance_counters;

    /// Should we add part level to produced chunk. Part level is useful for next steps if query has FINAL
    bool add_part_level = false;

    /// Shared context used for building indexes during query execution.
    MergeTreeIndexBuildContextPtr merge_tree_index_build_context;

    LoggerPtr log = getLogger("MergeTreeSelectProcessor");
    std::atomic<bool> is_cancelled{false};
};

}
