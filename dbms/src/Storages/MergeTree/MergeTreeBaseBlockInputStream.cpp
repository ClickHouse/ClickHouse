#include <Storages/MergeTree/MergeTreeBaseBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
}


MergeTreeBaseBlockInputStream::MergeTreeBaseBlockInputStream(
    MergeTreeData & storage,
    const ExpressionActionsPtr & prewhere_actions,
    const String & prewhere_column_name,
    size_t max_block_size_rows,
    size_t preferred_block_size_bytes,
    size_t preferred_max_column_in_block_size_bytes,
    size_t min_bytes_to_use_direct_io,
    size_t max_read_buffer_size,
    bool use_uncompressed_cache,
    bool save_marks_in_cache,
    const Names & virt_column_names)
:
    storage(storage),
    prewhere_actions(prewhere_actions),
    prewhere_column_name(prewhere_column_name),
    max_block_size_rows(max_block_size_rows),
    preferred_block_size_bytes(preferred_block_size_bytes),
    preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes),
    min_bytes_to_use_direct_io(min_bytes_to_use_direct_io),
    max_read_buffer_size(max_read_buffer_size),
    use_uncompressed_cache(use_uncompressed_cache),
    save_marks_in_cache(save_marks_in_cache),
    virt_column_names(virt_column_names),
    max_block_size_marks(max_block_size_rows / storage.index_granularity)
{
}


Block MergeTreeBaseBlockInputStream::readImpl()
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


Block MergeTreeBaseBlockInputStream::readFromPart()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const auto max_block_size_rows = this->max_block_size_rows;
    const auto preferred_block_size_bytes = this->preferred_block_size_bytes;
    const auto preferred_max_column_in_block_size_bytes = this->preferred_max_column_in_block_size_bytes;
    const auto index_granularity = storage.index_granularity;
    const double min_filtration_ratio = 0.00001;

    auto estimateNumRows = [preferred_block_size_bytes, max_block_size_rows,
        index_granularity, preferred_max_column_in_block_size_bytes, min_filtration_ratio](
        MergeTreeReadTask & task, MergeTreeRangeReader & reader)
    {
        if (!task.size_predictor)
            return max_block_size_rows;

        /// Calculates number of rows will be read using preferred_block_size_bytes.
        /// Can't be less than index_granularity.
        size_t rows_to_read = task.size_predictor->estimateNumRows(preferred_block_size_bytes);
        if (!rows_to_read)
            return rows_to_read;
        rows_to_read = std::max(index_granularity, rows_to_read);

        if (preferred_max_column_in_block_size_bytes)
        {
            /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
            size_t rows_to_read_for_max_size_column
                = task.size_predictor->estimateNumRowsForMaxSizeColumn(preferred_max_column_in_block_size_bytes);
            double filtration_ratio = std::max(min_filtration_ratio, 1.0 - task.size_predictor->filtered_rows_ratio);
            auto rows_to_read_for_max_size_column_with_filtration
                = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

            /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than index_granularity.
            rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
        }

        size_t unread_rows_in_current_granule = reader.numPendingRowsInCurrentGranule();
        if (unread_rows_in_current_granule >= rows_to_read)
            return rows_to_read;

        size_t granule_to_read = (rows_to_read + reader.numReadRowsInCurrentGranule() + index_granularity / 2) / index_granularity;
        return index_granularity * granule_to_read - reader.numReadRowsInCurrentGranule();
    };

    if (!task->range_reader.isInitialized())
    {
        if (prewhere_actions)
        {
            if (reader->getColumns().empty())
            {
                task->range_reader = MergeTreeRangeReader(
                        pre_reader.get(), index_granularity, nullptr, prewhere_actions,
                        &prewhere_column_name, &task->ordered_names,
                        task->should_reorder, task->remove_prewhere_column, true);
            }
            else
            {
                task->pre_range_reader = MergeTreeRangeReader(
                        pre_reader.get(), index_granularity, nullptr, prewhere_actions,
                        &prewhere_column_name, &task->ordered_names,
                        task->should_reorder, task->remove_prewhere_column, false);

                task->range_reader = MergeTreeRangeReader(
                        reader.get(), index_granularity, &task->pre_range_reader, nullptr,
                        nullptr, &task->ordered_names, true, false, true);
            }
        }
        else
        {
            task->range_reader = MergeTreeRangeReader(
                    reader.get(), index_granularity, nullptr, prewhere_actions,
                    nullptr, &task->ordered_names, task->should_reorder, false, true);
        }
    }

    size_t recommended_rows = estimateNumRows(*task, task->range_reader);
    size_t rows_to_read = std::max(static_cast<decltype(max_block_size_rows)>(1), std::min(max_block_size_rows, recommended_rows));

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.block.rows() == 0)
        read_result.block.clear();

    size_t num_filtered_rows = read_result.numReadRows() - read_result.block.rows();

    progressImpl({ read_result.numReadRows(), read_result.numBytesRead() });

    if (task->size_predictor)
    {
        task->size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);

        if (read_result.block)
            task->size_predictor->update(read_result.block);
    }

    if (read_result.block && prewhere_actions && !task->remove_prewhere_column)
    {
        /// Convert const column to full here because it's cheaper to filter const column than full.
        auto & column = read_result.block.getByName(prewhere_column_name);
        column.column = column.column->convertToFullColumnIfConst();
    }

    read_result.block.checkNumberOfRows();

    return read_result.block;
}


void MergeTreeBaseBlockInputStream::injectVirtualColumns(Block & block) const
{
    const auto rows = block.rows();

    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virt_column_names.empty())
    {
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
                    column = DataTypeUInt64().createColumnConst(rows, static_cast<UInt64>(task->part_index_in_query))->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                block.insert({ column, std::make_shared<DataTypeUInt64>(), virt_column_name});
            }
        }
    }
}


MergeTreeBaseBlockInputStream::~MergeTreeBaseBlockInputStream() = default;

}
