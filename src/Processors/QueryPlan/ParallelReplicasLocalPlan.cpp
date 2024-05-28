#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>

#include <Common/checkStackSize.h>
#include "Storages/MergeTree/RequestResponse.h"
#include <Interpreters/Context.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/ASTFunction.h>

#include <Storages/MergeTree/RequestResponse.h>

#include <memory>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
}

namespace DB
{

namespace
{

void addConvertingActions(QueryPlan & plan, const Block & header, bool has_missing_objects)
{
    if (blocksHaveEqualStructure(plan.getCurrentDataStream().header, header))
        return;

    auto mode = has_missing_objects ? ActionsDAG::MatchColumnsMode::Position : ActionsDAG::MatchColumnsMode::Name;

    auto get_converting_dag = [mode](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            mode,
            true);
    };

    auto convert_actions_dag = get_converting_dag(plan.getCurrentDataStream().header, header);
    auto converting = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), convert_actions_dag);
    plan.addStep(std::move(converting));
}

}

class ReadFromMergeTreeCoordinated : public ISourceStep
{
public:
    ReadFromMergeTreeCoordinated(QueryPlanStepPtr read_from_merge_tree_, ParallelReplicasReadingCoordinatorPtr coordinator_)
        : ISourceStep(read_from_merge_tree_->getOutputStream())
        , read_from_merge_tree(std::move(read_from_merge_tree_))
        , coordinator(std::move(coordinator_))
    {
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    String getName() const override { return "ReadFromLocalParallelReplica"; }

private:
    QueryPlanStepPtr read_from_merge_tree;
    ParallelReplicasReadingCoordinatorPtr coordinator;
};

void ReadFromMergeTreeCoordinated::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    ReadFromMergeTree & reading = *typeid_cast<ReadFromMergeTree *>(read_from_merge_tree.get());

    auto result = reading.getAnalysisResult();
    const auto & query_info = reading.getQueryInfo();
    const auto & data = reading.data;
    const auto & context = reading.getContext();
    const auto & storage_snapshot = reading.getStorageSnapshot();

    if (reading.enable_remove_parts_from_snapshot_optimization)
    {
        /// Do not keep data parts in snapshot.
        /// They are stored separately, and some could be released after PK analysis.
        reading.storage_snapshot->data = std::make_unique<MergeTreeData::SnapshotData>();
    }

    LOG_DEBUG(
        reading.log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);

    // Adding partition info to QueryAccessInfo.
    if (context->hasQueryContext() && !query_info.is_internal)
    {
        Names partition_names;
        for (const auto & part : result.parts_with_ranges)
        {
            partition_names.emplace_back(
                fmt::format("{}.{}", data.getStorageID().getFullNameNotQuoted(), part.data_part->info.partition_id));
        }
        context->getQueryContext()->addQueryAccessInfo(partition_names);

        if (storage_snapshot->projection)
            context->getQueryContext()->addQueryAccessInfo(
                Context::QualifiedProjectionName{.storage_id = data.getStorageID(), .projection_name = storage_snapshot->projection->name});
    }

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result, context);

    // TODO: check this on plan level, we should be here if there is nothing to read
    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    ActionsDAGPtr result_projection;

    Pipe pipe = reading.spreadMarkRanges(std::move(result.parts_with_ranges), reading.requested_num_streams, result, result_projection);

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    if (result.sampling.use_sampling)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header,
                sampling_actions,
                result.sampling.filter_function->getColumnName(),
                false);
        });
    }

    Block cur_header = pipe.getHeader();

    auto append_actions = [&result_projection](ActionsDAGPtr actions)
    {
        if (!result_projection)
            result_projection = std::move(actions);
        else
            result_projection = ActionsDAG::merge(std::move(*result_projection), std::move(*actions));
    };

    if (result_projection)
        cur_header = result_projection->updateHeader(cur_header);

    /// Extra columns may be returned (for example, if sampling is used).
    /// Convert pipe to step header structure.
    if (!isCompatibleHeader(cur_header, getOutputStream().header))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            cur_header.getColumnsWithTypeAndName(),
            getOutputStream().header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        append_actions(std::move(converting));
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(result_projection);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
        });
    }

    /// Some extra columns could be added by sample/final/in-order/etc
    /// Remove them from header if not needed.
    if (!blocksHaveEqualStructure(pipe.getHeader(), getOutputStream().header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            getOutputStream().header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            true);

        auto converting_dag_expr = std::make_shared<ExpressionActions>(convert_actions_dag);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, converting_dag_expr);
        });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
    pipeline.addContext(context);
    // Attach QueryIdHolder if needed
    if (query_id_holder)
        pipeline.setQueryIdHolder(std::move(query_id_holder));
}

std::unique_ptr<QueryPlan> createLocalPlanForParallelReplicas(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr read_from_merge_tree,
    bool has_missing_objects)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    auto new_context = Context::createCopy(context);

    /// Do not push down limit to local plan, as it will break `rows_before_limit_at_least` counter.
    if (processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit)
        processed_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage).ignoreASTOptimizations();

    /// For Analyzer, identifier in GROUP BY/ORDER BY/LIMIT BY lists has been resolved to
    /// ConstantNode in QueryTree if it is an alias of a constant, so we should not replace
    /// ConstantNode with ProjectionNode again(https://github.com/ClickHouse/ClickHouse/issues/62289).
    new_context->setSetting("enable_positional_arguments", Field(false));
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
    auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
    query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());

    QueryPlan::Node * node = query_plan->getRootNode();
    ReadFromMergeTree * reading = nullptr;
    while (node)
    {
        reading = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (reading)
            break;

        if (!node->children.empty())
            node = node->children.at(0);
        else
            node = nullptr;
    }

    chassert(reading);
    const auto * analyzed_merge_tree = typeid_cast<const ReadFromMergeTree *>(read_from_merge_tree.get());
    chassert(analyzed_merge_tree->hasAnalyzedResult());

    CoordinationMode mode = CoordinationMode::Default;
    switch (analyzed_merge_tree->getReadType())
    {
        case ReadFromMergeTree::ReadType::Default:
            mode = CoordinationMode::Default;
            break;
        case ReadFromMergeTree::ReadType::InOrder:
            mode = CoordinationMode::WithOrder;
            break;
        case ReadFromMergeTree::ReadType::InReverseOrder:
            mode = CoordinationMode::ReverseOrder;
            break;
        case ReadFromMergeTree::ReadType::ParallelReplicas:
            chassert(false);
            UNREACHABLE();
    }

    const auto number_of_local_replica = new_context->getSettingsRef().max_parallel_replicas - 1;
    coordinator->handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement(
        mode, analyzed_merge_tree->getAnalysisResult().parts_with_ranges.getDescriptions(), number_of_local_replica));

    MergeTreeAllRangesCallback all_ranges_cb = [coordinator](InitialAllRangesAnnouncement) {};

    MergeTreeReadTaskCallback read_task_cb = [coordinator](ParallelReadRequest req) -> std::optional<ParallelReadResponse>
    { return coordinator->handleRequest(std::move(req)); };

    auto read_from_merge_tree_parallel_replicas
        = reading->createLocalParallelReplicasReadingStep(analyzed_merge_tree, true, all_ranges_cb, read_task_cb, number_of_local_replica);
    node->step = std::move(read_from_merge_tree_parallel_replicas);

    addConvertingActions(*query_plan, header, has_missing_objects);
    return query_plan;
}

}
