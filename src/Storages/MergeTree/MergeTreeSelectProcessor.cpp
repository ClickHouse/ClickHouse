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
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    MergeTreeReadPoolPtr pool_,
    MergeTreeSelectAlgorithmPtr algorithm_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
    const MergeTreeReaderSettings & reader_settings_)
    : pool(std::move(pool_))
    , algorithm(std::move(algorithm_))
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , prewhere_actions(getPrewhereActions(prewhere_info, actions_settings, reader_settings_.enable_multiple_prewhere_read_steps))
    , reader_settings(reader_settings_)
    , block_size_params(block_size_params_)
    , result_header(transformHeader(pool->getHeader(), prewhere_info))
{
    if (reader_settings.apply_deleted_mask)
    {
        PrewhereExprStep step
        {
            .type = PrewhereExprStep::Filter,
            .actions = nullptr,
            .filter_column_name = RowExistsColumn::name,
            .remove_filter_column = true,
            .need_filter = true,
            .perform_alter_conversions = true,
        };

        lightweight_delete_filter_step = std::make_shared<PrewhereExprStep>(std::move(step));
    }

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

    task->initializeRangeReaders(all_prewhere_actions);
}

Block MergeTreeSelectProcessor::transformHeader(Block block, const PrewhereInfoPtr & prewhere_info)
{
    return SourceStepWithFilter::applyPrewhereActions(std::move(block), prewhere_info);
}

}
