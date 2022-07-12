#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Processors/Transforms/AggregatingTransform.h>


#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}


MergeTreeBaseSelectProcessor::MergeTreeBaseSelectProcessor(
    Block header,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    ExpressionActionsSettings actions_settings,
    UInt64 max_block_size_rows_,
    UInt64 preferred_block_size_bytes_,
    UInt64 preferred_max_column_in_block_size_bytes_,
    const MergeTreeReaderSettings & reader_settings_,
    bool use_uncompressed_cache_,
    const Names & virt_column_names_,
    std::optional<ParallelReadingExtension> extension_)
    : ISource(transformHeader(std::move(header), prewhere_info_, storage_.getPartitionValueType(), virt_column_names_))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(prewhere_info_)
    , max_block_size_rows(max_block_size_rows_)
    , preferred_block_size_bytes(preferred_block_size_bytes_)
    , preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes_)
    , reader_settings(reader_settings_)
    , use_uncompressed_cache(use_uncompressed_cache_)
    , virt_column_names(virt_column_names_)
    , partition_value_type(storage.getPartitionValueType())
    , extension(extension_)
{
    header_without_virtual_columns = getPort().getHeader();

    /// Reverse order is to minimize reallocations when removing columns from the block
    for (auto it = virt_column_names.rbegin(); it != virt_column_names.rend(); ++it)
    {
        if (*it == "_part_offset")
        {
            non_const_virtual_column_names.emplace_back(*it);
        }
        else if (*it == "__row_exists")
        {
            non_const_virtual_column_names.emplace_back(*it);
        }
        else
        {
            /// Remove virtual columns that are going to be filled with const values
            if (header_without_virtual_columns.has(*it))
                header_without_virtual_columns.erase(*it);
        }
    }

    if (prewhere_info)
    {
        prewhere_actions = std::make_unique<PrewhereExprInfo>();

        if (prewhere_info->row_level_filter)
        {
            PrewhereExprStep row_level_filter_step
            {
                .actions = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter, actions_settings),
                .column_name = prewhere_info->row_level_column_name,
                .remove_column = true,
                .need_filter = true
            };

            prewhere_actions->steps.emplace_back(std::move(row_level_filter_step));
        }

        PrewhereExprStep prewhere_step
        {
            .actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions, actions_settings),
            .column_name = prewhere_info->prewhere_column_name,
            .remove_column = prewhere_info->remove_prewhere_column,
            .need_filter = prewhere_info->need_filter
        };

        prewhere_actions->steps.emplace_back(std::move(prewhere_step));
    }
}


bool MergeTreeBaseSelectProcessor::getNewTask()
{
    /// No parallel reading feature
    if (!extension.has_value())
    {
        if (getNewTaskImpl())
        {
            finalizeNewTask();
            return true;
        }
        return false;
    }
    return getNewTaskParallelReading();
}


bool MergeTreeBaseSelectProcessor::getNewTaskParallelReading()
{
    if (getTaskFromBuffer())
        return true;

    if (no_more_tasks)
        return getDelayedTasks();

    while (true)
    {
        /// The end of execution. No task.
        if (!getNewTaskImpl())
        {
            no_more_tasks = true;
            return getDelayedTasks();
        }

        splitCurrentTaskRangesAndFillBuffer();

        if (getTaskFromBuffer())
            return true;
    }
}


bool MergeTreeBaseSelectProcessor::getTaskFromBuffer()
{
    while (!buffered_ranges.empty())
    {
        auto ranges = std::move(buffered_ranges.front());
        buffered_ranges.pop_front();

        assert(!ranges.empty());

        auto res = performRequestToCoordinator(ranges, /*delayed=*/false);

        if (Status::Accepted == res)
            return true;

        /// To avoid any possibility of ignoring cancellation, exception will be thrown.
        if (Status::Cancelled == res)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query had been cancelled");
    }
    return false;
}


bool MergeTreeBaseSelectProcessor::getDelayedTasks()
{
    while (!delayed_tasks.empty())
    {
        task = std::move(delayed_tasks.front());
        delayed_tasks.pop_front();

        assert(!task->mark_ranges.empty());

        auto res = performRequestToCoordinator(task->mark_ranges, /*delayed=*/true);

        if (Status::Accepted == res)
            return true;

        if (Status::Cancelled == res)
            break;
    }

    finish();
    return false;
}


Chunk MergeTreeBaseSelectProcessor::generate()
{
    while (!isCancelled())
    {
        try
        {
            if ((!task || task->isFinished()) && !getNewTask())
                return {};
        }
        catch (const Exception & e)
        {
            /// See MergeTreeBaseSelectProcessor::getTaskFromBuffer()
            if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                return {};
            throw;
        }

        auto res = readFromPart();

        if (res.hasRows())
        {
            injectVirtualColumns(res, task.get(), partition_value_type, virt_column_names);
            return res;
        }
    }

    return {};
}


void MergeTreeBaseSelectProcessor::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    MergeTreeRangeReader* prev_reader = nullptr;
    bool last_reader = false;
    size_t pre_readers_shift = 0;

    if (!reader_settings.skip_deleted_mask && current_task.data_part->getColumns().contains("__row_exists"))
    {
//        last_reader = !prewhere_actions || prewhere_actions->steps.empty();
        current_task.pre_range_readers.push_back(
            MergeTreeRangeReader(pre_reader_for_step[0].get(), prev_reader, &lwd_filter_step, last_reader, non_const_virtual_column_names));
        prev_reader = &current_task.pre_range_readers.back();
        pre_readers_shift++;
    }

    if (prewhere_info)
    {
        if (prewhere_actions->steps.size() + pre_readers_shift != pre_reader_for_step.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PREWHERE steps count mismatch, actions: {}, readers: {}",
                            prewhere_actions->steps.size(), pre_reader_for_step.size());


        for (size_t i = 0; i < prewhere_actions->steps.size(); ++i)
        {
            last_reader = reader->getColumns().empty() && (i + 1 == prewhere_actions->steps.size());
            current_task.pre_range_readers.push_back(
                MergeTreeRangeReader(pre_reader_for_step[i + pre_readers_shift].get(), prev_reader, &prewhere_actions->steps[i], last_reader, non_const_virtual_column_names));

            prev_reader = &current_task.pre_range_readers.back();
        }

    }

    if (!last_reader)
    {
        current_task.range_reader = MergeTreeRangeReader(reader.get(), prev_reader, nullptr, true, non_const_virtual_column_names);
    }
    else
    {
        /// If all columns are read by pre_range_readers than move last pre_range_reader into range_reader
        current_task.range_reader = std::move(current_task.pre_range_readers.back());
        current_task.pre_range_readers.pop_back();
    }
}

static UInt64 estimateNumRows(const MergeTreeReadTask & current_task, UInt64 current_preferred_block_size_bytes,
    UInt64 current_max_block_size_rows, UInt64 current_preferred_max_column_in_block_size_bytes, double min_filtration_ratio)
{
    const MergeTreeRangeReader & current_reader = current_task.range_reader;

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

    const MergeTreeIndexGranularity & index_granularity = current_task.data_part->index_granularity;

    return index_granularity.countMarksForRows(current_reader.currentMark(), rows_to_read, current_reader.numReadRowsInCurrentGranule());
}


Chunk MergeTreeBaseSelectProcessor::readFromPartImpl()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;
    const UInt64 current_preferred_block_size_bytes = preferred_block_size_bytes;
    const UInt64 current_preferred_max_column_in_block_size_bytes = preferred_max_column_in_block_size_bytes;
    const double min_filtration_ratio = 0.00001;

    UInt64 recommended_rows = estimateNumRows(*task, current_preferred_block_size_bytes,
        current_max_block_size_rows, current_preferred_max_column_in_block_size_bytes, min_filtration_ratio);
    UInt64 rows_to_read = std::max(static_cast<UInt64>(1), std::min(current_max_block_size_rows, recommended_rows));

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

    progress(read_result.numReadRows(), read_result.numBytesRead());

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
        const auto & name = header_without_virtual_columns.getByPosition(ps).name;
        if (name == "__row_exists" && !sample_block.has(name))
            continue; /// TODO: properly deal with cases when __row_exists is not read and is filled later 
        auto pos_in_sample_block = sample_block.getPositionByName(name);
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

        virtual void insertArrayOfStringsColumn(const ColumnPtr & column, const String & name) = 0;
        virtual void insertStringColumn(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUInt8Column(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUInt64Column(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUUIDColumn(const ColumnPtr & column, const String & name) = 0;

        virtual void insertPartitionValueColumn(
            size_t rows,
            const Row & partition_value,
            const DataTypePtr & partition_value_type,
            const String & name) = 0;
    };
}

/// Adds virtual columns that are not const for all rows
static void injectNonConstVirtualColumns(
    size_t rows,
    VirtualColumnsInserter & inserter,
    const Names & virtual_columns)
{
    if (unlikely(rows))
        throw Exception("Cannot insert non-constant virtual column to non-empty chunk.",
                        ErrorCodes::LOGICAL_ERROR);

    for (const auto & virtual_column_name : virtual_columns)
    {
        if (virtual_column_name == "_part_offset")
            inserter.insertUInt64Column(DataTypeUInt64().createColumn(), virtual_column_name);
        if (virtual_column_name == "__row_exists")
            inserter.insertUInt8Column(DataTypeUInt8().createColumn(), virtual_column_name);
    }
}

/// Adds virtual columns that are const for the whole part
static void injectPartConstVirtualColumns(
    size_t rows,
    VirtualColumnsInserter & inserter,
    MergeTreeReadTask * task,
    const DataTypePtr & partition_value_type,
    const Names & virtual_columns)
{
    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virtual_columns.empty())
    {
        if (unlikely(rows && !task))
            throw Exception("Cannot insert virtual columns to non-empty chunk without specified task.",
                            ErrorCodes::LOGICAL_ERROR);

        const IMergeTreeDataPart * part = nullptr;
        if (rows)
        {
            part = task->data_part.get();
            if (part->isProjectionPart())
                part = part->getParentPart();
        }
        for (const auto & virtual_column_name : virtual_columns)
        {
            if (virtual_column_name == "_part")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, part->name)->convertToFullColumnIfConst();
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
            else if (virtual_column_name == "_part_uuid")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUUID().createColumnConst(rows, part->uuid)->convertToFullColumnIfConst();
                else
                    column = DataTypeUUID().createColumn();

                inserter.insertUUIDColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_id")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, part->info.partition_id)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                inserter.insertStringColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_value")
            {
                if (rows)
                    inserter.insertPartitionValueColumn(rows, part->partition.value, partition_value_type, virtual_column_name);
                else
                    inserter.insertPartitionValueColumn(rows, {}, partition_value_type, virtual_column_name);
            }
        }
    }
}

namespace
{
    struct VirtualColumnsInserterIntoBlock : public VirtualColumnsInserter
    {
        explicit VirtualColumnsInserterIntoBlock(Block & block_) : block(block_) {}

        void insertArrayOfStringsColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), name});
        }

        void insertStringColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeString>(), name});
        }

        void insertUInt8Column(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUInt8>(), name});
        }

        void insertUInt64Column(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUInt64>(), name});
        }

        void insertUUIDColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUUID>(), name});
        }

        void insertPartitionValueColumn(
            size_t rows, const Row & partition_value, const DataTypePtr & partition_value_type, const String & name) final
        {
            ColumnPtr column;
            if (rows)
                column = partition_value_type->createColumnConst(rows, Tuple(partition_value.begin(), partition_value.end()))
                             ->convertToFullColumnIfConst();
            else
                column = partition_value_type->createColumn();

            block.insert({column, partition_value_type, name});
        }

        Block & block;
    };

    struct VirtualColumnsInserterIntoColumns : public VirtualColumnsInserter
    {
        explicit VirtualColumnsInserterIntoColumns(Columns & columns_) : columns(columns_) {}

        void insertArrayOfStringsColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertStringColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUInt8Column(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUInt64Column(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUUIDColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertPartitionValueColumn(
            size_t rows, const Row & partition_value, const DataTypePtr & partition_value_type, const String &) final
        {
            ColumnPtr column;
            if (rows)
                column = partition_value_type->createColumnConst(rows, Tuple(partition_value.begin(), partition_value.end()))
                             ->convertToFullColumnIfConst();
            else
                column = partition_value_type->createColumn();
            columns.push_back(column);
        }

        Columns & columns;
    };
}

void MergeTreeBaseSelectProcessor::injectVirtualColumns(
    Block & block, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    VirtualColumnsInserterIntoBlock inserter{block};

    /// First add non-const columns that are filled by the range reader and then const columns that we will fill ourselves.
    /// Note that the order is important: virtual columns filled by the range reader must go first
    injectNonConstVirtualColumns(block.rows(), inserter, virtual_columns);
    injectPartConstVirtualColumns(block.rows(), inserter, task, partition_value_type, virtual_columns);
}

void MergeTreeBaseSelectProcessor::injectVirtualColumns(
    Chunk & chunk, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    UInt64 num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    VirtualColumnsInserterIntoColumns inserter{columns};

/////////////////////////
// TODO: implement properly
    for (const auto & virtual_column_name : virtual_columns)
    {

            if (virtual_column_name == "__row_exists")
            {
                if (task->data_part->getColumns().contains(virtual_column_name))
                {
                    /// If this column is present in the part it must be read from the data
                    assert(task->task_columns.columns.contains(virtual_column_name));
                }
                else
                {
                    /// If __row_exists column isn't present in the part then  
                    ColumnPtr column;
                    if (num_rows)
                        column = DataTypeUInt8().createColumnConst(num_rows, 1)->convertToFullColumnIfConst();
                    else
                        column = DataTypeUInt8().createColumn();

                    inserter.insertUInt8Column(column, virtual_column_name);
                }
            }
    }
///////////////////////////    

    /// Only add const virtual columns because non-const ones have already been added
    injectPartConstVirtualColumns(num_rows, inserter, task, partition_value_type, virtual_columns);

    chunk.setColumns(columns, num_rows);
}

Block MergeTreeBaseSelectProcessor::transformHeader(
    Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    if (prewhere_info)
    {
        if (prewhere_info->row_level_filter)
        {
            block = prewhere_info->row_level_filter->updateHeader(std::move(block));
            auto & row_level_column = block.getByName(prewhere_info->row_level_column_name);
            if (!row_level_column.type->canBeUsedInBooleanContext())
            {
                throw Exception("Invalid type for filter in PREWHERE: " + row_level_column.type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
            }

            block.erase(prewhere_info->row_level_column_name);
        }

        if (prewhere_info->prewhere_actions)
            block = prewhere_info->prewhere_actions->updateHeader(std::move(block));

        auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);
        if (!prewhere_column.type->canBeUsedInBooleanContext())
        {
            throw Exception("Invalid type for filter in PREWHERE: " + prewhere_column.type->getName(),
                ErrorCodes::LOGICAL_ERROR);
        }

        if (prewhere_info->remove_prewhere_column)
            block.erase(prewhere_info->prewhere_column_name);
        else
        {
            WhichDataType which(removeNullable(recursiveRemoveLowCardinality(prewhere_column.type)));
            if (which.isInt() || which.isUInt())
                prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
            else if (which.isFloat())
                prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1.0f)->convertToFullColumnIfConst();
            else
                throw Exception("Illegal type " + prewhere_column.type->getName() + " of column for filter.",
                                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
        }
    }

    injectVirtualColumns(block, nullptr, partition_value_type, virtual_columns);
    return block;
}

std::unique_ptr<MergeTreeBlockSizePredictor> MergeTreeBaseSelectProcessor::getSizePredictor(
    const MergeTreeData::DataPartPtr & data_part,
    const MergeTreeReadTaskColumns & task_columns,
    const Block & sample_block)
{
    const auto & required_column_names = task_columns.columns.getNames();
    NameSet complete_column_names(required_column_names.begin(), required_column_names.end());
    for (const auto & pre_columns_per_step : task_columns.pre_columns)
    {
        const auto & required_pre_column_names = pre_columns_per_step.getNames();
        complete_column_names.insert(required_pre_column_names.begin(), required_pre_column_names.end());
    }

    return std::make_unique<MergeTreeBlockSizePredictor>(
        data_part, Names(complete_column_names.begin(), complete_column_names.end()), sample_block);
}


MergeTreeBaseSelectProcessor::Status MergeTreeBaseSelectProcessor::performRequestToCoordinator(MarkRanges requested_ranges, bool delayed)
{
    String partition_id = task->data_part->info.partition_id;
    String part_name;
    String projection_name;

    if (task->data_part->isProjectionPart())
    {
        part_name = task->data_part->getParentPart()->name;
        projection_name  = task->data_part->name;
    }
    else
    {
        part_name = task->data_part->name;
        projection_name = "";
    }

    PartBlockRange block_range
    {
        .begin = task->data_part->info.min_block,
        .end = task->data_part->info.max_block
    };

    PartitionReadRequest request
    {
        .partition_id = std::move(partition_id),
        .part_name = std::move(part_name),
        .projection_name = std::move(projection_name),
        .block_range = std::move(block_range),
        .mark_ranges = std::move(requested_ranges)
    };

    /// Constistent hashing won't work with reading in order, because at the end of the execution
    /// we could possibly seek back
    if (!delayed && canUseConsistentHashingForParallelReading())
    {
        const auto hash = request.getConsistentHash(extension->count_participating_replicas);
        if (hash != extension->number_of_current_replica)
        {
            auto delayed_task = std::make_unique<MergeTreeReadTask>(*task); // Create a copy
            delayed_task->mark_ranges = std::move(request.mark_ranges);
            delayed_tasks.emplace_back(std::move(delayed_task));
            return Status::Denied;
        }
    }

    auto optional_response = extension.value().callback(std::move(request));

    if (!optional_response.has_value())
        return Status::Cancelled;

    auto response = optional_response.value();

    task->mark_ranges = std::move(response.mark_ranges);

    if (response.denied || task->mark_ranges.empty())
        return Status::Denied;

    finalizeNewTask();

    return Status::Accepted;
}


size_t MergeTreeBaseSelectProcessor::estimateMaxBatchSizeForHugeRanges()
{
    /// This is an empirical number and it is so,
    /// because we have an adaptive granularity by default.
    const size_t average_granule_size_bytes = 8UL * 1024 * 1024 * 10; // 10 MiB

    /// We want to have one RTT per one gigabyte of data read from disk
    /// this could be configurable.
    const size_t max_size_for_one_request = 8UL * 1024 * 1024 * 1024; // 1 GiB

    size_t sum_average_marks_size = 0;
    /// getColumnSize is not fully implemented for compact parts
    if (task->data_part->getType() == IMergeTreeDataPart::Type::Compact)
    {
        sum_average_marks_size = average_granule_size_bytes;
    }
    else
    {
        for (const auto & name : extension->colums_to_read)
        {
            auto size = task->data_part->getColumnSize(name);

            assert(size.marks != 0);
            sum_average_marks_size += size.data_uncompressed / size.marks;
        }
    }

    if (sum_average_marks_size == 0)
        sum_average_marks_size = average_granule_size_bytes; // 10 MiB

    LOG_TEST(log, "Reading from {} part, average mark size is {}",
        task->data_part->getTypeName(), sum_average_marks_size);

    return max_size_for_one_request / sum_average_marks_size;
}

void MergeTreeBaseSelectProcessor::splitCurrentTaskRangesAndFillBuffer()
{
    const size_t max_batch_size = estimateMaxBatchSizeForHugeRanges();

    size_t current_batch_size = 0;
    buffered_ranges.emplace_back();

    for (const auto & range : task->mark_ranges)
    {
        auto expand_if_needed = [&]
        {
            if (current_batch_size > max_batch_size)
            {
                buffered_ranges.emplace_back();
                current_batch_size = 0;
            }
        };

        expand_if_needed();

        if (range.end - range.begin < max_batch_size)
        {
            buffered_ranges.back().push_back(range);
            current_batch_size += range.end - range.begin;
            continue;
        }

        auto current_begin = range.begin;
        auto current_end = range.begin + max_batch_size;

        while (current_end < range.end)
        {
            auto current_range = MarkRange{current_begin, current_end};
            buffered_ranges.back().push_back(current_range);
            current_batch_size += current_end - current_begin;

            current_begin = current_end;
            current_end = current_end + max_batch_size;

            expand_if_needed();
        }

        if (range.end - current_begin > 0)
        {
            auto current_range = MarkRange{current_begin, range.end};
            buffered_ranges.back().push_back(current_range);
            current_batch_size += range.end - current_begin;

            expand_if_needed();
        }
    }

    if (buffered_ranges.back().empty())
        buffered_ranges.pop_back();
}

MergeTreeBaseSelectProcessor::~MergeTreeBaseSelectProcessor() = default;

}
