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

namespace
{

template <typename Func>
struct TelemetryWrapper
{
    TelemetryWrapper(Func callback_, ProfileEvents::Event event_, std::string span_name_)
        : callback(std::move(callback_)), event(event_), span_name(std::move(span_name_))
    {
    }

    template <typename... Args>
    auto operator()(Args &&... args)
    {
        DB::OpenTelemetry::SpanHolder span(span_name);
        DB::ProfileEventTimeIncrement<DB::Time::Microseconds> increment(event);
        return callback(std::forward<Args>(args)...);
    }

private:
    Func callback;
    ProfileEvents::Event event;
    std::string span_name;
};

}

namespace ProfileEvents
{
extern const Event ParallelReplicasAnnouncementMicroseconds;
extern const Event ParallelReplicasReadRequestMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

ParallelReadingExtension::ParallelReadingExtension(
    MergeTreeAllRangesCallback all_callback_,
    MergeTreeReadTaskCallback callback_,
    size_t number_of_current_replica_,
    size_t total_nodes_count_)
    : number_of_current_replica(number_of_current_replica_), total_nodes_count(total_nodes_count_)
{
    all_callback = TelemetryWrapper<MergeTreeAllRangesCallback>{
        std::move(all_callback_), ProfileEvents::ParallelReplicasAnnouncementMicroseconds, "ParallelReplicasAnnouncement"};

    callback = TelemetryWrapper<MergeTreeReadTaskCallback>{
        std::move(callback_), ProfileEvents::ParallelReplicasReadRequestMicroseconds, "ParallelReplicasReadRequest"};
}

void ParallelReadingExtension::sendInitialRequest(CoordinationMode mode, const RangesInDataParts & ranges, size_t mark_segment_size) const
{
    all_callback(InitialAllRangesAnnouncement{mode, ranges.getDescriptions(), number_of_current_replica, mark_segment_size});
}

std::optional<ParallelReadResponse> ParallelReadingExtension::sendReadRequest(
    CoordinationMode mode, size_t min_number_of_marks, const RangesInDataPartsDescription & description) const
{
    return callback(ParallelReadRequest{mode, number_of_current_replica, min_number_of_marks, description});
}

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    MergeTreeReadPoolPtr pool_,
    MergeTreeSelectAlgorithmPtr algorithm_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_)
    : pool(std::move(pool_))
    , algorithm(std::move(algorithm_))
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , prewhere_actions(getPrewhereActions(prewhere_info, actions_settings, reader_settings_.enable_multiple_prewhere_read_steps))
    , reader_settings(reader_settings_)
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
            prewhere_info->prewhere_actions.dumpDAG(),
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
                .actions = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter->clone(), actions_settings),
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
                .actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions.clone(), actions_settings),
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

        auto res = algorithm->readFromTask(*task);

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

            auto chunk = Chunk(ordered_columns, res.row_count);
            if (add_part_level)
                chunk.getChunkInfos().add(std::make_shared<MergeTreePartLevelInfo>(task->getInfo().data_part->info.level));

            return ChunkAndProgress{
                .chunk = std::move(chunk),
                .num_read_rows = res.num_read_rows,
                .num_read_bytes = res.num_read_bytes,
                .is_finished = false};
        }

        return {Chunk(), res.num_read_rows, res.num_read_bytes, false};
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
