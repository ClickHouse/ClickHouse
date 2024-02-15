#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Processors/Merges/Algorithms/MergeTreePartLevelInfo.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/BlockNumberColumn.h>
#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

static void injectNonConstVirtualColumns(
    size_t rows,
    Block & block,
    const Names & virtual_columns,
    MergeTreeReadTask * task = nullptr);

static void injectPartConstVirtualColumns(
    size_t rows,
    Block & block,
    MergeTreeReadTask * task,
    const DataTypePtr & partition_value_type,
    const Names & virtual_columns);

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    MergeTreeReadPoolPtr pool_,
    MergeTreeSelectAlgorithmPtr algorithm_,
    const MergeTreeData & storage_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_)
    : pool(std::move(pool_))
    , algorithm(std::move(algorithm_))
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , prewhere_actions(getPrewhereActions(prewhere_info, actions_settings, reader_settings_.enable_multiple_prewhere_read_steps))
    , reader_settings(reader_settings_)
    , block_size_params(block_size_params_)
    , virt_column_names(virt_column_names_)
    , partition_value_type(storage_.getPartitionValueType())
{
    if (reader_settings.apply_deleted_mask)
    {
        PrewhereExprStep step
        {
            .type = PrewhereExprStep::Filter,
            .actions = nullptr,
            .filter_column_name = LightweightDeleteDescription::FILTER_COLUMN.name,
            .remove_filter_column = true,
            .need_filter = true,
            .perform_alter_conversions = true,
        };

        lightweight_delete_filter_step = std::make_shared<PrewhereExprStep>(std::move(step));
    }

    header_without_const_virtual_columns = applyPrewhereActions(pool->getHeader(), prewhere_info);
    size_t non_const_columns_offset = header_without_const_virtual_columns.columns();
    injectNonConstVirtualColumns(0, header_without_const_virtual_columns, virt_column_names);

    for (size_t col_num = non_const_columns_offset; col_num < header_without_const_virtual_columns.columns(); ++col_num)
        non_const_virtual_column_names.emplace_back(header_without_const_virtual_columns.getByPosition(col_num).name);

    result_header = header_without_const_virtual_columns;
    injectPartConstVirtualColumns(0, result_header, nullptr, partition_value_type, virt_column_names);

    if (!prewhere_actions.steps.empty())
        LOG_TRACE(log, "PREWHERE condition was split into {} steps: {}", prewhere_actions.steps.size(), prewhere_actions.dumpConditions());

    if (prewhere_info)
        LOG_TEST(log, "Original PREWHERE DAG:\n{}\nPREWHERE actions:\n{}",
            (prewhere_info->prewhere_actions ? prewhere_info->prewhere_actions->dumpDAG(): std::string("<nullptr>")),
            (!prewhere_actions.steps.empty() ? prewhere_actions.dump() : std::string("<nullptr>")));
}

String MergeTreeSelectProcessor::getName() const
{
    return fmt::format("MergeTreeSelect(pool: {}, algorithm: {})", pool->getName(), algorithm->getName());
}

bool tryBuildPrewhereSteps(PrewhereInfoPtr prewhere_info, const ExpressionActionsSettings & actions_settings, PrewhereExprInfo & prewhere);

PrewhereExprInfo MergeTreeSelectProcessor::getPrewhereActions(PrewhereInfoPtr prewhere_info, const ExpressionActionsSettings & actions_settings, bool enable_multiple_prewhere_read_steps)
{
    PrewhereExprInfo prewhere_actions;
    if (prewhere_info)
    {
        if (prewhere_info->row_level_filter)
        {
            PrewhereExprStep row_level_filter_step
            {
                .type = PrewhereExprStep::Filter,
                .actions = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter, actions_settings),
                .filter_column_name = prewhere_info->row_level_column_name,
                .remove_filter_column = true,
                .need_filter = true,
                .perform_alter_conversions = true,
            };

            prewhere_actions.steps.emplace_back(std::make_shared<PrewhereExprStep>(std::move(row_level_filter_step)));
        }

        if (!enable_multiple_prewhere_read_steps ||
            !tryBuildPrewhereSteps(prewhere_info, actions_settings, prewhere_actions))
        {
            PrewhereExprStep prewhere_step
            {
                .type = PrewhereExprStep::Filter,
                .actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions, actions_settings),
                .filter_column_name = prewhere_info->prewhere_column_name,
                .remove_filter_column = prewhere_info->remove_prewhere_column,
                .need_filter = prewhere_info->need_filter,
                .perform_alter_conversions = true,
            };

            prewhere_actions.steps.emplace_back(std::make_shared<PrewhereExprStep>(std::move(prewhere_step)));
        }
    }

    return prewhere_actions;
}

ChunkAndProgress MergeTreeSelectProcessor::read()
{
    while (!is_cancelled)
    {
        try
        {
            if (!task || algorithm->needNewTask(*task))
                task = algorithm->getNewTask(*pool, task.get());

            if (!task)
                break;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                break;
            throw;
        }

        if (!task->getMainRangeReader().isInitialized())
            initializeRangeReaders();

        auto res = algorithm->readFromTask(*task, block_size_params);

        if (res.row_count)
        {
            injectVirtualColumns(res.block, res.row_count, task.get(), partition_value_type, virt_column_names);

            /// Reorder the columns according to result_header
            Columns ordered_columns;
            ordered_columns.reserve(result_header.columns());
            for (size_t i = 0; i < result_header.columns(); ++i)
            {
                auto name = result_header.getByPosition(i).name;
                ordered_columns.push_back(res.block.getByName(name).column);
            }

            return ChunkAndProgress{
                .chunk = Chunk(ordered_columns, res.row_count, add_part_level ? std::make_shared<MergeTreePartLevelInfo>(task->getInfo().data_part->info.level) : nullptr),
                .num_read_rows = res.num_read_rows,
                .num_read_bytes = res.num_read_bytes,
                .is_finished = false};
        }
        else
        {
            return {Chunk(), res.num_read_rows, res.num_read_bytes, false};
        }
    }

    return {Chunk(), 0, 0, true};
}

void MergeTreeSelectProcessor::initializeRangeReaders()
{
    PrewhereExprInfo all_prewhere_actions;
    if (lightweight_delete_filter_step && task->getInfo().data_part->hasLightweightDelete())
        all_prewhere_actions.steps.push_back(lightweight_delete_filter_step);

    for (const auto & step : prewhere_actions.steps)
        all_prewhere_actions.steps.push_back(step);

    task->initializeRangeReaders(all_prewhere_actions, non_const_virtual_column_names);
}


namespace
{
    struct VirtualColumnsInserter
    {
        explicit VirtualColumnsInserter(Block & block_) : block(block_) {}

        bool columnExists(const String & name) const { return block.has(name); }

        void insertUInt8Column(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeUInt8>(), name});
        }

        void insertUInt64Column(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeUInt64>(), name});
        }

        void insertUUIDColumn(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeUUID>(), name});
        }

        void insertLowCardinalityColumn(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), name});
        }

        void insertPartitionValueColumn(
            size_t rows, const Row & partition_value, const DataTypePtr & partition_value_type, const String & name)
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
}

/// Adds virtual columns that are not const for all rows
static void injectNonConstVirtualColumns(
    size_t rows,
    Block & block,
    const Names & virtual_columns,
    MergeTreeReadTask * task)
{
    VirtualColumnsInserter inserter(block);
    for (const auto & virtual_column_name : virtual_columns)
    {
        if (virtual_column_name == "_part_offset")
        {
            if (!rows)
            {
                inserter.insertUInt64Column(DataTypeUInt64().createColumn(), virtual_column_name);
            }
            else
            {
                if (!inserter.columnExists(virtual_column_name))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Column {} must have been filled part reader",
                        virtual_column_name);
            }
        }

        if (virtual_column_name == LightweightDeleteDescription::FILTER_COLUMN.name)
        {
                /// If _row_exists column isn't present in the part then fill it here with 1s
                ColumnPtr column;
                if (rows)
                    column = LightweightDeleteDescription::FILTER_COLUMN.type->createColumnConst(rows, 1)->convertToFullColumnIfConst();
                else
                    column = LightweightDeleteDescription::FILTER_COLUMN.type->createColumn();

                inserter.insertUInt8Column(column, virtual_column_name);
        }

        if (virtual_column_name == BlockNumberColumn::name)
        {
            ColumnPtr column;
            if (rows)
            {
                size_t value = 0;
                if (task)
                {
                    value = task->getInfo().data_part ? task->getInfo().data_part->info.min_block : 0;
                }
                column = BlockNumberColumn::type->createColumnConst(rows, value)->convertToFullColumnIfConst();
            }
            else
                column = BlockNumberColumn::type->createColumn();

            inserter.insertUInt64Column(column, virtual_column_name);
        }
    }
}

/// Adds virtual columns that are const for the whole part
static void injectPartConstVirtualColumns(
    size_t rows,
    Block & block,
    MergeTreeReadTask * task,
    const DataTypePtr & partition_value_type,
    const Names & virtual_columns)
{
    VirtualColumnsInserter inserter(block);
    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virtual_columns.empty())
    {
        if (unlikely(rows && !task))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert virtual columns to non-empty chunk without specified task.");

        const IMergeTreeDataPart * part = nullptr;

        if (rows)
        {
            part = task->getInfo().data_part.get();
            if (part->isProjectionPart())
                part = part->getParentPart();
        }

        for (const auto & virtual_column_name : virtual_columns)
        {
            if (virtual_column_name == "_part")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}
                                 .createColumnConst(rows, part->name)
                                 ->convertToFullColumnIfConst();
                else
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn();

                inserter.insertLowCardinalityColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, task->getInfo().part_index_in_query)->convertToFullColumnIfConst();
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
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}
                                 .createColumnConst(rows, part->info.partition_id)
                                 ->convertToFullColumnIfConst();
                else
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn();

                inserter.insertLowCardinalityColumn(column, virtual_column_name);
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

void MergeTreeSelectProcessor::injectVirtualColumns(
    Block & block, size_t row_count, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    /// First add non-const columns that are filled by the range reader and then const columns that we will fill ourselves.
    /// Note that the order is important: virtual columns filled by the range reader must go first
    injectNonConstVirtualColumns(row_count, block, virtual_columns,task);
    injectPartConstVirtualColumns(row_count, block, task, partition_value_type, virtual_columns);
}

Block MergeTreeSelectProcessor::applyPrewhereActions(Block block, const PrewhereInfoPtr & prewhere_info)
{
    if (prewhere_info)
    {
        if (prewhere_info->row_level_filter)
        {
            block = prewhere_info->row_level_filter->updateHeader(std::move(block));
            auto & row_level_column = block.getByName(prewhere_info->row_level_column_name);
            if (!row_level_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Invalid type for filter in PREWHERE: {}",
                    row_level_column.type->getName());
            }

            block.erase(prewhere_info->row_level_column_name);
        }

        if (prewhere_info->prewhere_actions)
        {
            block = prewhere_info->prewhere_actions->updateHeader(std::move(block));

            auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);
            if (!prewhere_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Invalid type for filter in PREWHERE: {}",
                    prewhere_column.type->getName());
            }

            if (prewhere_info->remove_prewhere_column)
            {
                block.erase(prewhere_info->prewhere_column_name);
            }
            else if (prewhere_info->need_filter)
            {
                WhichDataType which(removeNullable(recursiveRemoveLowCardinality(prewhere_column.type)));

                if (which.isNativeInt() || which.isNativeUInt())
                    prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
                else if (which.isFloat())
                    prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1.0f)->convertToFullColumnIfConst();
                else
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                        "Illegal type {} of column for filter",
                        prewhere_column.type->getName());
            }
        }
    }

    return block;
}

Block MergeTreeSelectProcessor::transformHeader(
    Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    injectVirtualColumns(block, 0, nullptr, partition_value_type, virtual_columns);
    auto transformed = applyPrewhereActions(std::move(block), prewhere_info);
    return transformed;
}

}
