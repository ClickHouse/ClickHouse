#pragma once
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct MergeTreeData;
struct MergeTreeReadTask;
class MergeTreeBlockSizePredictor;


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

    std::shared_ptr<MergeTreeBlockSizePredictor> size_predictor;

    MergeTreeReadTask(
        const MergeTreeData::DataPartPtr & data_part, const MarkRanges & mark_ranges, const std::size_t part_index_in_query,
        const Names & ordered_names, const NameSet & column_name_set, const NamesAndTypesList & columns,
        const NamesAndTypesList & pre_columns, const bool remove_prewhere_column, const bool should_reorder)
    : data_part{data_part}, mark_ranges{mark_ranges}, part_index_in_query{part_index_in_query},
    ordered_names{ordered_names}, column_name_set{column_name_set}, columns{columns}, pre_columns{pre_columns},
    remove_prewhere_column{remove_prewhere_column}, should_reorder{should_reorder}
    {}
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;


struct MergeTreeBlockSizePredictor
{
    MergeTreeBlockSizePredictor(const MergeTreeData & storage, MergeTreeReadTask & task)
    : task{task}
    {
        index_granularity = storage.index_granularity;
    }

    void init()
    {
        auto add_column = [&] (const NameAndTypePair & column)
        {
            ColumnPtr column_data = column.type->createColumn();
            const auto column_checksum = task.data_part->tryGetBinChecksum(column.name);

            /// There are no data files, column will be const
            if (!column_checksum)
                return;

            if (column_data->isFixed())
            {
                fixed_columns_bytes_per_row += column_data->sizeOfField();
            }
            else
            {
                ColumnInfo info;
                info.name = column.name;
                info.bytes_per_row_global = column_checksum->uncompressed_size;

                dynamic_columns_infos.emplace_back(info);
            }
        };

        for (const NameAndTypePair & column : task.pre_columns)
            add_column(column);

        for (const NameAndTypePair & column : task.columns)
            add_column(column);

        size_t rows_approx = task.data_part->tryGetExactSizeRows().first;

        bytes_per_row_global = fixed_columns_bytes_per_row;
        for (auto & info : dynamic_columns_infos)
        {
            info.bytes_per_row_global /= rows_approx;
            info.bytes_per_row = info.bytes_per_row_global;
            bytes_per_row_global += info.bytes_per_row_global;
        }
    }

    void startBlock()
    {
        if (!is_initialized)
        {
            is_initialized = true;
            init();
        }

        block_size_bytes = 0;
        block_size_rows = 0;
        for (auto & info : dynamic_columns_infos)
            info.size_bytes = 0;
    }

    void update(const Block & block, size_t read_marks)
    {
        size_t dif_rows = read_marks * index_granularity;
        block_size_rows += dif_rows;
        block_size_bytes = block_size_rows * fixed_columns_bytes_per_row;
        bytes_per_row_current = fixed_columns_bytes_per_row;

        for (auto & info : dynamic_columns_infos)
        {
            size_t new_size = block.getByName(info.name).column->byteSize();
            size_t dif_size = new_size - info.size_bytes;

            double local_bytes_per_row = static_cast<double>(dif_size) / dif_rows;

            /// Make recursive updates for each read mark
            for (size_t i = 0; i < read_marks; ++i)
                info.bytes_per_row +=  decay * (local_bytes_per_row - info.bytes_per_row);

            info.size_bytes = new_size;
            block_size_bytes += new_size;
            bytes_per_row_current += info.bytes_per_row;
        }
    }

    size_t estimateByteSize(size_t num_marks)
    {
        return static_cast<size_t>(bytes_per_row_current * num_marks * index_granularity);
    }

    size_t estimateNumMarks(size_t bytes_quota)
    {
        return static_cast<size_t>(bytes_quota / bytes_per_row_current / index_granularity);
    }

    MergeTreeReadTask & task;
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
