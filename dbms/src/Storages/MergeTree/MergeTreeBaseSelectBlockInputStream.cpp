#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
}


MergeTreeBaseSelectBlockInputProcessor::MergeTreeBaseSelectBlockInputProcessor(
    Block header,
    const MergeTreeData & storage_,
    const PrewhereInfoPtr & prewhere_info_,
    UInt64 max_block_size_rows_,
    UInt64 preferred_block_size_bytes_,
    UInt64 preferred_max_column_in_block_size_bytes_,
    UInt64 min_bytes_to_use_direct_io_,
    UInt64 max_read_buffer_size_,
    bool use_uncompressed_cache_,
    bool save_marks_in_cache_,
    const Names & virt_column_names_)
:
    ISource(getHeader(std::move(header), prewhere_info_, virt_column_names_)),
    storage(storage_),
    prewhere_info(prewhere_info_),
    max_block_size_rows(max_block_size_rows_),
    preferred_block_size_bytes(preferred_block_size_bytes_),
    preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes_),
    min_bytes_to_use_direct_io(min_bytes_to_use_direct_io_),
    max_read_buffer_size(max_read_buffer_size_),
    use_uncompressed_cache(use_uncompressed_cache_),
    save_marks_in_cache(save_marks_in_cache_),
    virt_column_names(virt_column_names_)
{
}


Chunk MergeTreeBaseSelectBlockInputProcessor::generate()
{
    while (!isCancelled())
    {
        if ((!task || task->isFinished()) && !getNewTask())
            return {};

        auto res = readFromPart();

        if (!res.hasNoRows())
        {
            injectVirtualColumns(res, task.get(), virt_column_names);
            return res;
        }
    }

    return {};
}


void MergeTreeBaseSelectBlockInputProcessor::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    if (prewhere_info)
    {
        if (reader->getColumns().empty())
        {
            current_task.range_reader = MergeTreeRangeReader(
                pre_reader.get(), nullptr,
                prewhere_info->alias_actions, prewhere_info->prewhere_actions,
                &prewhere_info->prewhere_column_name, &current_task.ordered_names,
                current_task.should_reorder, current_task.remove_prewhere_column, true);
        }
        else
        {
            MergeTreeRangeReader * pre_reader_ptr = nullptr;
            if (pre_reader != nullptr)
            {
                current_task.pre_range_reader = MergeTreeRangeReader(
                    pre_reader.get(), nullptr,
                    prewhere_info->alias_actions, prewhere_info->prewhere_actions,
                    &prewhere_info->prewhere_column_name, &current_task.ordered_names,
                    current_task.should_reorder, current_task.remove_prewhere_column, false);
                pre_reader_ptr = &current_task.pre_range_reader;
            }

            current_task.range_reader = MergeTreeRangeReader(
                reader.get(), pre_reader_ptr, nullptr, nullptr,
                nullptr, &current_task.ordered_names, true, false, true);
        }
    }
    else
    {
        current_task.range_reader = MergeTreeRangeReader(
            reader.get(), nullptr, nullptr, nullptr,
            nullptr, &current_task.ordered_names, current_task.should_reorder, false, true);
    }
}


Chunk MergeTreeBaseSelectBlockInputProcessor::readFromPartImpl()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;
    const UInt64 current_preferred_block_size_bytes = preferred_block_size_bytes;
    const UInt64 current_preferred_max_column_in_block_size_bytes = preferred_max_column_in_block_size_bytes;
    const MergeTreeIndexGranularity & index_granularity = task->data_part->index_granularity;
    const double min_filtration_ratio = 0.00001;

    auto estimateNumRows = [current_preferred_block_size_bytes, current_max_block_size_rows,
        &index_granularity, current_preferred_max_column_in_block_size_bytes, min_filtration_ratio](
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

    UInt64 recommended_rows = estimateNumRows(*task, task->range_reader);
    UInt64 rows_to_read = std::max(UInt64(1), std::min(current_max_block_size_rows, recommended_rows));

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    UInt64 num_filtered_rows = read_result.numReadRows() - read_result.num_rows;

    /// TODO
    /// progressImpl({ read_result.numReadRows(), read_result.numBytesRead() });

    if (task->size_predictor)
    {
        task->size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);

        if (!read_result.columns.empty())
            task->size_predictor->update(read_result.columns);
    }

    return Chunk(std::move(read_result.columns), read_result.num_rows);
}


Chunk MergeTreeBaseSelectBlockInputProcessor::readFromPart()
{
    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    return readFromPartImpl();
}


template <typename InsertCallback>
static void injectVirtualColumnsImpl(size_t rows, InsertCallback & callback, MergeTreeReadTask * task, const Names & virtual_columns)
{
    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virtual_columns.empty())
    {
        if (unlikely(rows && !task))
            throw Exception("Cannot insert virtual columns to non-empty chunk without specified task.",
                            ErrorCodes::LOGICAL_ERROR);

        for (const auto & virtual_column_name : virtual_columns)
        {
            if (virtual_column_name == "_part")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, task->data_part->name)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                callback.template insert<DataTypeString>(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, task->part_index_in_query)->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                callback.template insert<DataTypeUInt64>(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_id")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, task->data_part->info.partition_id)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                callback.template insert<DataTypeString>(column, virtual_column_name);
            }
        }
    }
}

namespace
{
    struct InsertIntoBlockCallback
    {
        template <typename DataType>
        void insert(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataType>(), name});
        }

        Block & block;
    };

    struct InsertIntoColumnsCallback
    {
        template <typename>
        void insert(const ColumnPtr & column, const String &)
        {
            columns.push_back(column);
        }

        Columns & columns;
    };
}

void MergeTreeBaseSelectBlockInputProcessor::injectVirtualColumns(Block & block, MergeTreeReadTask * task, const Names & virtual_columns)
{
    InsertIntoBlockCallback callback { block };
    injectVirtualColumnsImpl(block.rows(), callback, task, virtual_columns);
}

void MergeTreeBaseSelectBlockInputProcessor::injectVirtualColumns(Chunk & chunk, MergeTreeReadTask * task, const Names & virtual_columns)
{
    UInt64 num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    InsertIntoColumnsCallback callback { columns };
    injectVirtualColumnsImpl(num_rows, callback, task, virtual_columns);

    chunk.setColumns(columns, num_rows);
}

void MergeTreeBaseSelectBlockInputProcessor::executePrewhereActions(Block & block, const PrewhereInfoPtr & prewhere_info)
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

Block MergeTreeBaseSelectBlockInputProcessor::getHeader(
    Block block, const PrewhereInfoPtr & prewhere_info, const Names & virtual_columns)
{
    executePrewhereActions(block, prewhere_info);
    injectVirtualColumns(block, nullptr, virtual_columns);
    return block;
}


MergeTreeBaseSelectBlockInputProcessor::~MergeTreeBaseSelectBlockInputProcessor() = default;

}
