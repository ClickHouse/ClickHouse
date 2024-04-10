#pragma once
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <boost/core/noncopyable.hpp>


namespace DB
{

struct PrewhereExprInfo;

struct ChunkAndProgress
{
    Chunk chunk;
    size_t num_read_rows = 0;
    size_t num_read_bytes = 0;
    /// Explicitly indicate that we have read all data.
    /// This is needed to occasionally return empty chunk to indicate the progress while the rows are filtered out in PREWHERE.
    bool is_finished = false;
};

struct ParallelReadingExtension
{
    MergeTreeAllRangesCallback all_callback;
    MergeTreeReadTaskCallback callback;
    size_t count_participating_replicas{0};
    size_t number_of_current_replica{0};
    /// This is needed to estimate the number of bytes
    /// between a pair of marks to perform one request
    /// over the network for a 1Gb of data.
    Names columns_to_read;
};

/// Base class for MergeTreeThreadSelectAlgorithm and MergeTreeSelectAlgorithm
class MergeTreeSelectProcessor : private boost::noncopyable
{
public:
    MergeTreeSelectProcessor(
        MergeTreeReadPoolPtr pool_,
        MergeTreeSelectAlgorithmPtr algorithm_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReadTask::BlockSizeParams & block_size_params_,
        const MergeTreeReaderSettings & reader_settings_);

    String getName() const;

    static Block transformHeader(Block block, const PrewhereInfoPtr & prewhere_info);
    Block getHeader() const { return result_header; }

    ChunkAndProgress read();

    void cancel() { is_cancelled = true; }

    const MergeTreeReaderSettings & getSettings() const { return reader_settings; }

    static PrewhereExprInfo getPrewhereActions(
        PrewhereInfoPtr prewhere_info,
        const ExpressionActionsSettings & actions_settings,
        bool enable_multiple_prewhere_read_steps);

    void addPartLevelToChunk(bool add_part_level_) { add_part_level = add_part_level_; }

private:
    /// Sets up range readers corresponding to data readers
    void initializeRangeReaders();

    const MergeTreeReadPoolPtr pool;
    const MergeTreeSelectAlgorithmPtr algorithm;
    const StorageSnapshotPtr storage_snapshot;

    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const PrewhereExprInfo prewhere_actions;

    const MergeTreeReaderSettings reader_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;

    /// Current task to read from.
    MergeTreeReadTaskPtr task;
    /// This step is added when the part has lightweight delete mask
    PrewhereExprStepPtr lightweight_delete_filter_step;
    /// A result of getHeader(). A chunk which this header is returned from read().
    Block result_header;

    /// Should we add part level to produced chunk. Part level is useful for next steps if query has FINAL
    bool add_part_level = false;

    LoggerPtr log = getLogger("MergeTreeSelectProcessor");
    std::atomic<bool> is_cancelled{false};
};

using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

}
