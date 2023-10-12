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
        const MergeTreeData & storage_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReadTask::BlockSizeParams & block_size_params_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & virt_column_names_);

    String getName() const;

    static Block transformHeader(
        Block block,
        const PrewhereInfoPtr & prewhere_info,
        const DataTypePtr & partition_value_type,
        const Names & virtual_columns);

    Block getHeader() const { return result_header; }

    ChunkAndProgress read();

    void cancel() { is_cancelled = true; }

    const MergeTreeReaderSettings & getSettings() const { return reader_settings; }

    static PrewhereExprInfo getPrewhereActions(
        PrewhereInfoPtr prewhere_info,
        const ExpressionActionsSettings & actions_settings,
        bool enable_multiple_prewhere_read_steps);

private:
    /// This struct allow to return block with no columns but with non-zero number of rows similar to Chunk
    struct BlockAndProgress
    {
        Block block;
        size_t row_count = 0;
        size_t num_read_rows = 0;
        size_t num_read_bytes = 0;
    };

    /// Used for filling header with no rows as well as block with data
    static void injectVirtualColumns(Block & block, size_t row_count, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns);
    static Block applyPrewhereActions(Block block, const PrewhereInfoPtr & prewhere_info);

    /// Sets up range readers corresponding to data readers
    void initializeRangeReaders();

    const MergeTreeReadPoolPtr pool;
    const MergeTreeSelectAlgorithmPtr algorithm;

    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const PrewhereExprInfo prewhere_actions;

    const MergeTreeReaderSettings reader_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;
    const Names virt_column_names;
    const DataTypePtr partition_value_type;

    /// Current task to read from.
    MergeTreeReadTaskPtr task;
    /// This step is added when the part has lightweight delete mask
    PrewhereExprStepPtr lightweight_delete_filter_step;
    /// These columns will be filled by the merge tree range reader
    Names non_const_virtual_column_names;
    /// This header is used for chunks from readFromPart().
    Block header_without_const_virtual_columns;
    /// A result of getHeader(). A chunk which this header is returned from read().
    Block result_header;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeSelectProcessor");
    std::atomic<bool> is_cancelled{false};
};

using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

}
