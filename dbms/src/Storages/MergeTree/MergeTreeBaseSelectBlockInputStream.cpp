#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <Common/StackTrace.h>
#include <ext/range.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
}


MergeTreeBaseSelectBlockInputStream::MergeTreeBaseSelectBlockInputStream(
    const MergeTreeData & storage,
    const PrewhereInfoPtr & prewhere_info,
    UInt64 max_block_size_rows,
    UInt64 preferred_block_size_bytes,
    UInt64 preferred_max_column_in_block_size_bytes,
    UInt64 min_bytes_to_use_direct_io,
    UInt64 max_read_buffer_size,
    bool use_uncompressed_cache,
    bool save_marks_in_cache,
    const Names & virt_column_names)
:
    storage(storage),
    prewhere_info(prewhere_info),
    max_block_size_rows(max_block_size_rows),
    preferred_block_size_bytes(preferred_block_size_bytes),
    preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes),
    min_bytes_to_use_direct_io(min_bytes_to_use_direct_io),
    max_read_buffer_size(max_read_buffer_size),
    use_uncompressed_cache(use_uncompressed_cache),
    save_marks_in_cache(save_marks_in_cache),
    virt_column_names(virt_column_names)
{
}


Block MergeTreeBaseSelectBlockInputStream::readImpl()
{
    Block res;

    while (!res && !isCancelled())
    {
        if (!task && !getNewTask())
            break;

        res = readFromPart();

        if (res)
            injectVirtualColumns(res);

        if (task->isFinished())
            task.reset();
    }

    return res;
}


Block MergeTreeBaseSelectBlockInputStream::readFromPart()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const auto current_max_block_size_rows = max_block_size_rows;
    const auto current_preferred_block_size_bytes = preferred_block_size_bytes;
    const auto current_preferred_max_column_in_block_size_bytes = preferred_max_column_in_block_size_bytes;
    const auto & index_granularity = task->data_part->index_granularity;
    const double min_filtration_ratio = 0.00001;

    auto estimateNumRows = [current_preferred_block_size_bytes, current_max_block_size_rows,
        index_granularity, current_preferred_max_column_in_block_size_bytes, min_filtration_ratio](
        MergeTreeReadTask & current_task, MergeTreeRangeReader & current_reader)
    {
        if (!current_task.size_predictor)
            return static_cast<size_t>(current_max_block_size_rows);

        /// Calculates number of rows will be read using preferred_block_size_bytes.
        /// Can't be less than avg_index_granularity.
        size_t rows_to_read = current_task.size_predictor->estimateNumRows(current_preferred_block_size_bytes);
        if (!rows_to_read)
            return rows_to_read;
        auto total_row_in_current_granule = current_reader.numRowsInCurrentGranule();
        rows_to_read = std::max(total_row_in_current_granule, rows_to_read);

        if (current_preferred_max_column_in_block_size_bytes)
        {
            /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
            auto rows_to_read_for_max_size_column
                = current_task.size_predictor->estimateNumRowsForMaxSizeColumn(current_preferred_max_column_in_block_size_bytes);
            double filtration_ratio = std::max(min_filtration_ratio, 1.0 - current_task.size_predictor->filtered_rows_ratio);
            auto rows_to_read_for_max_size_column_with_filtration
                = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

            /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than current_index_granularity.
            rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
        }

        auto unread_rows_in_current_granule = current_reader.numPendingRowsInCurrentGranule();
        if (unread_rows_in_current_granule >= rows_to_read)
            return rows_to_read;

        return index_granularity.countMarksForRows(current_reader.currentMark(), rows_to_read, current_reader.numReadRowsInCurrentGranule());
    };

    if (!task->range_reader.isInitialized())
    {
        if (prewhere_info)
        {
            if (reader->getColumns().empty())
            {
                task->range_reader = MergeTreeRangeReader(
                    pre_reader.get(), nullptr,
                    prewhere_info->alias_actions, prewhere_info->prewhere_actions,
                    &prewhere_info->prewhere_column_name, &task->ordered_names,
                    task->should_reorder, task->remove_prewhere_column, true);
            }
            else
            {
                MergeTreeRangeReader * pre_reader_ptr = nullptr;
                if (pre_reader != nullptr)
                {
                    task->pre_range_reader = MergeTreeRangeReader(
                        pre_reader.get(), nullptr,
                        prewhere_info->alias_actions, prewhere_info->prewhere_actions,
                        &prewhere_info->prewhere_column_name, &task->ordered_names,
                        task->should_reorder, task->remove_prewhere_column, false);
                    pre_reader_ptr = &task->pre_range_reader;
                }

                task->range_reader = MergeTreeRangeReader(
                    reader.get(), pre_reader_ptr, nullptr, nullptr,
                    nullptr, &task->ordered_names, true, false, true);
            }
        }
        else
        {
            task->range_reader = MergeTreeRangeReader(
                reader.get(),  nullptr, nullptr, nullptr,
                nullptr, &task->ordered_names, task->should_reorder, false, true);
        }
    }

    UInt64 recommended_rows = estimateNumRows(*task, task->range_reader);
    UInt64 rows_to_read = std::max(UInt64(1), std::min(current_max_block_size_rows, recommended_rows));

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.block.rows() == 0)
        read_result.block.clear();

    UInt64 num_filtered_rows = read_result.numReadRows() - read_result.block.rows();

    progressImpl({ read_result.numReadRows(), read_result.numBytesRead() });

    if (task->size_predictor)
    {
        task->size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);

        if (read_result.block)
            task->size_predictor->update(read_result.block);
    }

    if (read_result.block && prewhere_info && !task->remove_prewhere_column)
    {
        /// Convert const column to full here because it's cheaper to filter const column than full.
        auto & column = read_result.block.getByName(prewhere_info->prewhere_column_name);
        column.column = column.column->convertToFullColumnIfConst();
    }

    read_result.block.checkNumberOfRows();

    return read_result.block;
}


void MergeTreeBaseSelectBlockInputStream::injectVirtualColumns(Block & block) const
{
    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virt_column_names.empty())
    {
        const auto rows = block.rows();

        for (const auto & virt_column_name : virt_column_names)
        {
            if (virt_column_name == "_part")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, task->data_part->name)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                block.insert({ column, std::make_shared<DataTypeString>(), virt_column_name});
            }
            else if (virt_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, task->part_index_in_query)->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                block.insert({ column, std::make_shared<DataTypeUInt64>(), virt_column_name});
            }
            else if (virt_column_name == "_partition_id")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, task->data_part->info.partition_id)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                block.insert({ column, std::make_shared<DataTypeString>(), virt_column_name});
            }
        }
    }
}


void MergeTreeBaseSelectBlockInputStream::executePrewhereActions(Block & block, const PrewhereInfoPtr & prewhere_info)
{
    if (prewhere_info)
    {
        if (prewhere_info->alias_actions)
            prewhere_info->alias_actions->execute(block);

        prewhere_info->prewhere_actions->execute(block);
        if (prewhere_info->remove_prewhere_column)
            block.erase(prewhere_info->prewhere_column_name);

        if (!block)
            block.insert({nullptr, std::make_shared<DataTypeNothing>(), "_nothing"});
    }
}


MergeTreeBaseSelectBlockInputStream::~MergeTreeBaseSelectBlockInputStream() = default;

}
