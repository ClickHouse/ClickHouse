#pragma once

#include <optional>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>


namespace DB
{

class MergeTreeData;
struct MergeTreeReadTask;
struct MergeTreeBlockSizePredictor;

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;
using MergeTreeBlockSizePredictorPtr = std::unique_ptr<MergeTreeBlockSizePredictor>;


/** If some of the requested columns are not in the part,
  * then find out which columns may need to be read further,
  * so that you can calculate the DEFAULT expression for these columns.
  * Adds them to the `columns`.
  */
NameSet injectRequiredColumns(const MergeTreeData & storage, const MergeTreeData::DataPartPtr & part, Names & columns);


/// A batch of work for MergeTreeThreadSelectBlockInputStream
struct MergeTreeReadTask
{
    /// data part which should be read while performing this task
    MergeTreeData::DataPartPtr data_part;
    /// Ranges to read from `data_part`.
    MarkRanges mark_ranges;
    /// for virtual `part_index` virtual column
    size_t part_index_in_query;
    /// ordered list of column names used in this query, allows returning blocks with consistent ordering
    const Names & ordered_names;
    /// used to determine whether column should be filtered during PREWHERE or WHERE
    const NameSet & column_name_set;
    /// column names to read during WHERE
    const NamesAndTypesList & columns;
    /// column names to read during PREWHERE
    const NamesAndTypesList & pre_columns;
    /// should PREWHERE column be returned to requesting side?
    const bool remove_prewhere_column;
    /// resulting block may require reordering in accordance with `ordered_names`
    const bool should_reorder;
    /// Used to satistfy preferred_block_size_bytes limitation
    MergeTreeBlockSizePredictorPtr size_predictor;
    /// Used to save current range processing status
    MergeTreeRangeReader range_reader;
    MergeTreeRangeReader pre_range_reader;

    bool isFinished() const { return mark_ranges.empty() && range_reader.isCurrentRangeFinished(); }

    MergeTreeReadTask(
        const MergeTreeData::DataPartPtr & data_part_, const MarkRanges & mark_ranges_, const size_t part_index_in_query_,
        const Names & ordered_names_, const NameSet & column_name_set_, const NamesAndTypesList & columns_,
        const NamesAndTypesList & pre_columns_, const bool remove_prewhere_column_, const bool should_reorder_,
        MergeTreeBlockSizePredictorPtr && size_predictor_);

    virtual ~MergeTreeReadTask();
};

struct MergeTreeReadTaskColumns
{
    /// column names to read during WHERE
    NamesAndTypesList columns;
    /// column names to read during PREWHERE
    NamesAndTypesList pre_columns;
    /// resulting block may require reordering in accordance with `ordered_names`
    bool should_reorder;
};

MergeTreeReadTaskColumns getReadTaskColumns(const MergeTreeData & storage, const MergeTreeData::DataPartPtr & data_part,
    const Names & required_columns, const PrewhereInfoPtr & prewhere_info, bool check_columns);

struct MergeTreeBlockSizePredictor
{
    MergeTreeBlockSizePredictor(const MergeTreeData::DataPartPtr & data_part_, const Names & columns, const Block & sample_block);

    /// Reset some values for correct statistics calculating
    void startBlock();

    /// Updates statistic for more accurate prediction
    void update(const Block & sample_block, const Columns & columns, size_t num_rows, double decay = DECAY());

    /// Return current block size (after update())
    inline size_t getBlockSize() const
    {
        return block_size_bytes;
    }


    /// Predicts what number of rows should be read to exhaust byte quota per column
    inline size_t estimateNumRowsForMaxSizeColumn(size_t bytes_quota) const
    {
        double max_size_per_row = std::max<double>(std::max<size_t>(max_size_per_row_fixed, 1), max_size_per_row_dynamic);
        return (bytes_quota > block_size_rows * max_size_per_row)
            ? static_cast<size_t>(bytes_quota / max_size_per_row) - block_size_rows
            : 0;
    }

    /// Predicts what number of rows should be read to exhaust byte quota per block
    inline size_t estimateNumRows(size_t bytes_quota) const
    {
        return (bytes_quota > block_size_bytes)
            ? static_cast<size_t>((bytes_quota - block_size_bytes) / std::max<size_t>(1, bytes_per_row_current))
            : 0;
    }

    inline void updateFilteredRowsRation(size_t rows_was_read, size_t rows_was_filtered, double decay = DECAY())
    {
        double alpha = std::pow(1. - decay, rows_was_read);
        double current_ration = rows_was_filtered / std::max(1.0, static_cast<double>(rows_was_read));
        filtered_rows_ratio = current_ration < filtered_rows_ratio
            ? current_ration
            : alpha * filtered_rows_ratio + (1.0 - alpha) * current_ration;
    }

    /// Aggressiveness of bytes_per_row updates. See update() implementation.
    /// After n=NUM_UPDATES_TO_TARGET_WEIGHT updates v_{n} = (1 - TARGET_WEIGHT) * v_{0} + TARGET_WEIGHT * v_{target}
    static constexpr double TARGET_WEIGHT = 0.5;
    static constexpr size_t NUM_UPDATES_TO_TARGET_WEIGHT = 8192;
    static double DECAY() { return 1. - std::pow(TARGET_WEIGHT, 1. / NUM_UPDATES_TO_TARGET_WEIGHT); }

protected:

    MergeTreeData::DataPartPtr data_part;

    struct ColumnInfo
    {
        String name;
        double bytes_per_row_global = 0;
        double bytes_per_row = 0;
        size_t size_bytes = 0;
    };

    std::vector<ColumnInfo> dynamic_columns_infos;
    size_t fixed_columns_bytes_per_row = 0;

    size_t max_size_per_row_fixed = 0;
    double max_size_per_row_dynamic = 0;

    size_t number_of_rows_in_part;

    bool is_initialized_in_update = false;

    void initialize(const Block & sample_block, const Columns & columns, const Names & names, bool from_update = false);

public:

    size_t block_size_bytes = 0;
    size_t block_size_rows = 0;

    /// Total statistics
    double bytes_per_row_current = 0;
    double bytes_per_row_global = 0;

    double filtered_rows_ratio = 0;
};

}
