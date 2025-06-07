#pragma once

#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>

#include <Processors/Chunk.h>

#include <boost/core/noncopyable.hpp>


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

private:
    MergeTreeAllRangesCallback all_callback;
    MergeTreeReadTaskCallback callback;
    const size_t number_of_current_replica;
    const size_t total_nodes_count;
};

using ProjectionIndexReadRangesByIndex = std::unordered_map<size_t, RangesInDataParts>;

class MergeTreeSelectProcessor;
using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

struct ProjectionIndexBitmap;
using ProjectionIndexBitmapPtr = std::shared_ptr<ProjectionIndexBitmap>;
class MergeTreeReadPoolProjectionIndex;

struct ProjectionIndexReader
{
    ProjectionDescriptionRawPtr projection;
    std::shared_ptr<MergeTreeReadPoolProjectionIndex> projection_index_read_pool;
    MergeTreeSelectProcessorPtr processor;

    ProjectionIndexReader(
        ProjectionDescriptionRawPtr projection_,
        std::shared_ptr<MergeTreeReadPoolProjectionIndex> pool,
        PrewhereInfoPtr prewhere_info,
        const ExpressionActionsSettings & actions_settings,
        const MergeTreeReaderSettings & reader_settings);

    /// Reads and builds the ProjectionIndexBitmap for a given set of ranges in a data part.
    /// The bitmap is lazily constructed and memoized per part using a shared future.
    ///
    /// This function ensures:
    /// - Only one thread builds the bitmap for a part; others wait on the result.
    /// - Efficient use of 32-bit or 64-bit bitmap based on part offset size.
    /// - Proper handling of cancellation or exceptions, with early return when applicable.
    /// - If cancelled or an error occurs, returns nullptr — signaling that the projection index
    ///   should not be used for this data part.
    ProjectionIndexBitmapPtr getOrBuildProjectionIndexBitmapFromRanges(const RangesInDataPart & ranges) const;

    /// Cleans up the cached ProjectionIndexBitmap for a given part if it exists.
    /// Should be called when the last task for the part has finished.
    void cleanupProjectionIndexBitmap(const RangesInDataPart & ranges) const;

    void cancel() const noexcept;
};

using ProjectionIndexReaderByName = std::unordered_map<String, ProjectionIndexReader>;

/// A simple wrapper to allow atomic counters to be mutated even when accessed through a const map.
struct MutableAtomicSizeT
{
    mutable std::atomic_size_t value;
};
using PartRemainingMarks = std::unordered_map<size_t, MutableAtomicSizeT>;

/// Holds the necessary context to build projection index bitmaps during query execution.
struct ProjectionIndexBuildContext
{
    /// For each part, stores a set of read ranges grouped by projection.
    const ProjectionIndexReadRangesByIndex read_ranges;

    /// Maps projection names to their corresponding readers.
    const ProjectionIndexReaderByName readers;

    /// Tracks how many marks are still being processed for each part during the execution phase. Once the count reaches
    /// zero for a part, its cached index can be released to free resources.
    const PartRemainingMarks part_remaining_marks;
};
using ProjectionIndexBuildContextPtr = std::shared_ptr<ProjectionIndexBuildContext>;

/// Base class for MergeTreeThreadSelectAlgorithm and MergeTreeSelectAlgorithm
class MergeTreeSelectProcessor : private boost::noncopyable
{
public:
    MergeTreeSelectProcessor(
        MergeTreeReadPoolPtr pool_,
        MergeTreeSelectAlgorithmPtr algorithm_,
        const PrewhereInfoPtr & prewhere_info_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        ProjectionIndexBuildContextPtr projection_index_build_context_ = {});

    String getName() const;

    static Block transformHeader(
        Block block,
        const LazilyReadInfoPtr & lazily_read_info,
        const PrewhereInfoPtr & prewhere_info);

    Block getHeader() const { return result_header; }

    ChunkAndProgress read();

    void cancel() noexcept;

    const MergeTreeReaderSettings & getSettings() const { return reader_settings; }

    static PrewhereExprInfo getPrewhereActions(
        PrewhereInfoPtr prewhere_info,
        const ExpressionActionsSettings & actions_settings,
        bool enable_multiple_prewhere_read_steps,
        bool force_short_circuit_execution);

    void addPartLevelToChunk(bool add_part_level_) { add_part_level = add_part_level_; }

    void onFinish() const;

private:
    friend struct ProjectionIndexReader;

    static void injectLazilyReadColumns(
        size_t rows,
        Block & block,
        size_t part_index,
        const LazilyReadInfoPtr & lazily_read_info);

    /// Sets up range readers corresponding to data readers
    void initializeReadersChain();

    const MergeTreeReadPoolPtr pool;
    const MergeTreeSelectAlgorithmPtr algorithm;

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

    /// Shared context used for building projection indexes during query execution.
    ProjectionIndexBuildContextPtr projection_index_build_context;

    LoggerPtr log = getLogger("MergeTreeSelectProcessor");
    std::atomic<bool> is_cancelled{false};
};

}
