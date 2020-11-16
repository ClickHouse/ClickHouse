#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MergeTreeBaseSelectProcessor::MergeTreeBaseSelectProcessor(
    Block header,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    UInt64 max_block_size_rows_,
    UInt64 preferred_block_size_bytes_,
    UInt64 preferred_max_column_in_block_size_bytes_,
    const MergeTreeReaderSettings & reader_settings_,
    bool use_uncompressed_cache_,
    const Names & virt_column_names_)
    : SourceWithProgress(getHeader(std::move(header), prewhere_info_, virt_column_names_))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , prewhere_info(prewhere_info_)
    , max_block_size_rows(max_block_size_rows_)
    , preferred_block_size_bytes(preferred_block_size_bytes_)
    , preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes_)
    , reader_settings(reader_settings_)
    , use_uncompressed_cache(use_uncompressed_cache_)
    , virt_column_names(virt_column_names_)
{
    header_without_virtual_columns = getPort().getHeader();

    for (auto it = virt_column_names.rbegin(); it != virt_column_names.rend(); ++it)
        if (header_without_virtual_columns.has(*it))
            header_without_virtual_columns.erase(*it);
}


Chunk MergeTreeBaseSelectProcessor::generate()
{
    while (!isCancelled())
    {
        if ((!task || task->isFinished()) && !getNewTask())
            return {};

        auto res = readFromPart();

        if (res.hasRows())
        {
            injectVirtualColumns(res, task.get(), virt_column_names);
            return res;
        }
    }

    return {};
}


void MergeTreeBaseSelectProcessor::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    if (prewhere_info)
    {
        if (reader->getColumns().empty())
        {
            current_task.range_reader = MergeTreeRangeReader(pre_reader.get(), nullptr, prewhere_info, true);
        }
        else
        {
            MergeTreeRangeReader * pre_reader_ptr = nullptr;
            if (pre_reader != nullptr)
            {
                current_task.pre_range_reader = MergeTreeRangeReader(pre_reader.get(), nullptr, prewhere_info, false);
                pre_reader_ptr = &current_task.pre_range_reader;
            }

            current_task.range_reader = MergeTreeRangeReader(reader.get(), pre_reader_ptr, nullptr, true);
        }
    }
    else
    {
        current_task.range_reader = MergeTreeRangeReader(reader.get(), nullptr, nullptr, true);
    }
}


Chunk MergeTreeBaseSelectProcessor::readFromPartImpl()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;
    const UInt64 current_preferred_block_size_bytes = preferred_block_size_bytes;
    const UInt64 current_preferred_max_column_in_block_size_bytes = preferred_max_column_in_block_size_bytes;
    const MergeTreeIndexGranularity & index_granularity = task->data_part->index_granularity;
    const double min_filtration_ratio = 0.00001;

    auto estimate_num_rows = [current_preferred_block_size_bytes, current_max_block_size_rows,
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

    UInt64 recommended_rows = estimate_num_rows(*task, task->range_reader);
    UInt64 rows_to_read = std::max(UInt64(1), std::min(current_max_block_size_rows, recommended_rows));

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    const auto & sample_block = task->range_reader.getSampleBlock();
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception("Inconsistent number of columns got from MergeTreeRangeReader. "
                        "Have " + toString(sample_block.columns()) + " in sample block "
                        "and " + toString(read_result.columns.size()) + " columns in list", ErrorCodes::LOGICAL_ERROR);

    /// TODO: check columns have the same types as in header.

    UInt64 num_filtered_rows = read_result.numReadRows() - read_result.num_rows;

    progress({ read_result.numReadRows(), read_result.numBytesRead() });

    if (task->size_predictor)
    {
        task->size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);

        if (!read_result.columns.empty())
            task->size_predictor->update(sample_block, read_result.columns, read_result.num_rows);
    }

    if (read_result.num_rows == 0)
        return {};

    Columns ordered_columns;
    ordered_columns.reserve(header_without_virtual_columns.columns());

    /// Reorder columns. TODO: maybe skip for default case.
    for (size_t ps = 0; ps < header_without_virtual_columns.columns(); ++ps)
    {
        auto pos_in_sample_block = sample_block.getPositionByName(header_without_virtual_columns.getByPosition(ps).name);
        ordered_columns.emplace_back(std::move(read_result.columns[pos_in_sample_block]));
    }

    return Chunk(std::move(ordered_columns), read_result.num_rows);
}


Chunk MergeTreeBaseSelectProcessor::readFromPart()
{
    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    return readFromPartImpl();
}


namespace
{
    /// Simple interfaces to insert virtual columns.
    struct VirtualColumnsInserter
    {
        virtual ~VirtualColumnsInserter() = default;

        virtual void insertStringColumn(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUInt64Column(const ColumnPtr & column, const String & name) = 0;
    };
}

static void injectVirtualColumnsImpl(size_t rows, VirtualColumnsInserter & inserter,
                                     MergeTreeReadTask * task, const Names & virtual_columns)
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

                inserter.insertStringColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, task->part_index_in_query)->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                inserter.insertUInt64Column(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_id")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, task->data_part->info.partition_id)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                inserter.insertStringColumn(column, virtual_column_name);
            }
        }
    }
}

namespace
{
    struct VirtualColumnsInserterIntoBlock : public VirtualColumnsInserter
    {
        explicit VirtualColumnsInserterIntoBlock(Block & block_) : block(block_) {}

        void insertStringColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeString>(), name});
        }

        void insertUInt64Column(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUInt64>(), name});
        }

        Block & block;
    };

    struct VirtualColumnsInserterIntoColumns : public VirtualColumnsInserter
    {
        explicit VirtualColumnsInserterIntoColumns(Columns & columns_) : columns(columns_) {}

        void insertStringColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUInt64Column(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        Columns & columns;
    };
}

void MergeTreeBaseSelectProcessor::injectVirtualColumns(Block & block, MergeTreeReadTask * task, const Names & virtual_columns)
{
    VirtualColumnsInserterIntoBlock inserter { block };
    injectVirtualColumnsImpl(block.rows(), inserter, task, virtual_columns);
}

void MergeTreeBaseSelectProcessor::injectVirtualColumns(Chunk & chunk, MergeTreeReadTask * task, const Names & virtual_columns)
{
    UInt64 num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    VirtualColumnsInserterIntoColumns inserter { columns };
    injectVirtualColumnsImpl(num_rows, inserter, task, virtual_columns);

    chunk.setColumns(columns, num_rows);
}

void MergeTreeBaseSelectProcessor::executePrewhereActions(Block & block, const PrewhereInfoPtr & prewhere_info)
{
    if (prewhere_info)
    {
        if (prewhere_info->alias_actions)
            prewhere_info->alias_actions->execute(block);

        prewhere_info->prewhere_actions->execute(block);
        auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);

        if (!prewhere_column.type->canBeUsedInBooleanContext())
            throw Exception("Invalid type for filter in PREWHERE: " + prewhere_column.type->getName(),
                            ErrorCodes::LOGICAL_ERROR);

        if (prewhere_info->remove_prewhere_column)
            block.erase(prewhere_info->prewhere_column_name);
        else
        {
            auto & ctn = block.getByName(prewhere_info->prewhere_column_name);
            ctn.column = ctn.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
        }
    }
}

Block MergeTreeBaseSelectProcessor::getHeader(
    Block block, const PrewhereInfoPtr & prewhere_info, const Names & virtual_columns)
{
    executePrewhereActions(block, prewhere_info);
    injectVirtualColumns(block, nullptr, virtual_columns);
    return block;
}


MergeTreeBaseSelectProcessor::~MergeTreeBaseSelectProcessor() = default;

}
