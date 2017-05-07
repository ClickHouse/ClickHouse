#pragma once
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

class MergeTreeData;
struct MergeTreeReadTask;
struct MergeTreeBlockSizePredictor;

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;
using MergeTreeBlockSizePredictorPtr = std::shared_ptr<MergeTreeBlockSizePredictor>;


/** If some of the requested columns are not in the part,
  * then find out which columns may need to be read further,
  * so that you can calculate the DEFAULT expression for these columns.
  * Adds them to the `columns`.
  */
NameSet injectRequiredColumns(const MergeTreeData & storage, const MergeTreeData::DataPartPtr & part, Names & columns);


/// A batch of work for MergeTreeThreadBlockInputStream
struct MergeTreeReadTask
{
    /// data part which should be read while performing this task
    MergeTreeData::DataPartPtr data_part;
    /** Ranges to read from `data_part`.
     *    Specified in reverse order for MergeTreeThreadBlockInputStream's convenience of calling .pop_back(). */
    MarkRanges mark_ranges;
    /// for virtual `part_index` virtual column
    std::size_t part_index_in_query;
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

    MergeTreeReadTask(
        const MergeTreeData::DataPartPtr & data_part, const MarkRanges & mark_ranges, const std::size_t part_index_in_query,
        const Names & ordered_names, const NameSet & column_name_set, const NamesAndTypesList & columns,
        const NamesAndTypesList & pre_columns, const bool remove_prewhere_column, const bool should_reorder,
        const MergeTreeBlockSizePredictorPtr & size_predictor);

    virtual ~MergeTreeReadTask();
};


struct MergeTreeBlockSizePredictor
{
    MergeTreeBlockSizePredictor(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns,
        const NamesAndTypesList & pre_columns);

    /// Reset some values for correct statistics calculating
    void startBlock();

    /// Updates statistic for more accurate prediction
    void update(const Block & block, double decay = DECAY());

    /// Return current block size (after update())
    inline size_t getBlockSize() const
    {
        return block_size_bytes;
    }

    /// Predicts what number of rows should be read to exhaust byte quota
    inline size_t estimateNumRows(size_t bytes_quota) const
    {
        return (bytes_quota > block_size_bytes)
            ? static_cast<size_t>((bytes_quota - block_size_bytes) / bytes_per_row_current)
            : 0;
    }

    /// Predicts what number of marks should be read to exhaust byte quota
    inline size_t estimateNumMarks(size_t bytes_quota, size_t index_granularity) const
    {
        return (estimateNumRows(bytes_quota) + index_granularity / 2) / index_granularity;
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

public:

    size_t block_size_bytes = 0;
    size_t block_size_rows = 0;

    /// Total statistics
    double bytes_per_row_current = 0;
    double bytes_per_row_global = 0;
};

}
