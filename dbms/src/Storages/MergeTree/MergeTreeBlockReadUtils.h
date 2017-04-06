#pragma once
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct MergeTreeData;
struct MergeTreeReadTask;
class MergeTreeBlockSizePredictor;

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;
using MergeTreeBlockSizePredictorPtr = std::shared_ptr<MergeTreeBlockSizePredictor>;


/** Если некоторых запрошенных столбцов нет в куске,
    *  то выясняем, какие столбцы может быть необходимо дополнительно прочитать,
    *  чтобы можно было вычислить DEFAULT выражение для этих столбцов.
    * Добавляет их в columns.
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
        const NamesAndTypesList & pre_columns,
        size_t index_granularity_);

    void startBlock();

    /// TODO: take into account gaps between adjacent marks
    void update(const Block & block, size_t read_marks);

    size_t estimateByteSize(size_t num_marks)
    {
        return static_cast<size_t>(bytes_per_row_current * num_marks * index_granularity);
    }

    size_t estimateNumMarks(size_t bytes_quota)
    {
        return static_cast<size_t>(bytes_quota / bytes_per_row_current / index_granularity);
    }

    MergeTreeData::DataPartPtr data_part;
    bool is_initialized = false;
    size_t index_granularity;
    const double decay = 0.75;

    struct ColumnInfo
    {
        String name;
        double bytes_per_row_global = 0;
        double bytes_per_row = 0;
        size_t size_bytes = 0;
    };

    std::vector<ColumnInfo> dynamic_columns_infos;
    size_t fixed_columns_bytes_per_row = 0;

    size_t block_size_bytes = 0;
    size_t block_size_rows = 0;

    /// Total statistics
    double bytes_per_row_current = 0;
    double bytes_per_row_global = 0;
};

}
