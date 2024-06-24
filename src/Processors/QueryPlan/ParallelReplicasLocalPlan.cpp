#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>

#include <Common/checkStackSize.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RequestResponse.h>

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

std::unique_ptr<QueryPlan> createLocalPlanForParallelReplicas(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr analyzed_read_from_merge_tree,
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

    ReadFromMergeTree * analyzed_merge_tree = nullptr;
    if (analyzed_read_from_merge_tree.get())
        analyzed_merge_tree = typeid_cast<ReadFromMergeTree *>(analyzed_read_from_merge_tree.get());

    MergeTreeAllRangesCallback all_ranges_cb
        = [coordinator](InitialAllRangesAnnouncement announcement) { coordinator->handleInitialAllRangesAnnouncement(announcement); };

    MergeTreeReadTaskCallback read_task_cb = [coordinator](ParallelReadRequest req) -> std::optional<ParallelReadResponse>
    { return coordinator->handleRequest(std::move(req)); };

    const auto number_of_local_replica = 0;
    auto read_from_merge_tree_parallel_replicas
        = reading->createLocalParallelReplicasReadingStep(analyzed_merge_tree, all_ranges_cb, read_task_cb, number_of_local_replica);
    node->step = std::move(read_from_merge_tree_parallel_replicas);

    addConvertingActions(*query_plan, header, has_missing_objects);
    return query_plan;
}

}
