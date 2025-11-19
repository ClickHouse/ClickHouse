#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>

#include <base/sleep.h>
#include <Common/checkStackSize.h>
#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ConvertingActions.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{

namespace FailPoints
{
    extern const char slowdown_parallel_replicas_local_plan_read[];
}

std::pair<QueryPlanPtr, bool> createLocalPlanForParallelReplicas(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr analyzed_read_from_merge_tree,
    size_t replica_number)
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
        {
            // in case of RIGHT JOIN, - reading from right table is parallelized among replicas
            const JoinStep * join = typeid_cast<JoinStep *>(node->step.get());
            const JoinStepLogical * join_logical = typeid_cast<JoinStepLogical *>(node->step.get());
            if ((join && join->getJoin()->getTableJoin().kind() == JoinKind::Right)
             || (join_logical && join_logical->getJoinOperator().kind == JoinKind::Right))
                node = node->children.at(1);
            else
                node = node->children.at(0);
        }
        else
            node = nullptr;
    }

    if (!reading)
        /// it can happened if merge tree table is empty, - it'll be replaced with ReadFromPreparedSource
        return {std::move(query_plan), false};

    ReadFromMergeTree::AnalysisResultPtr analyzed_result_ptr;
    if (analyzed_read_from_merge_tree.get())
    {
        auto * analyzed_merge_tree = typeid_cast<ReadFromMergeTree *>(analyzed_read_from_merge_tree.get());
        if (analyzed_merge_tree)
            analyzed_result_ptr = analyzed_merge_tree->getAnalyzedResult();
    }

    MergeTreeAllRangesCallback all_ranges_cb = [coordinator](InitialAllRangesAnnouncement announcement)
    { coordinator->handleInitialAllRangesAnnouncement(std::move(announcement)); };

    MergeTreeReadTaskCallback read_task_cb = [coordinator](ParallelReadRequest req) -> std::optional<ParallelReadResponse>
    {
        fiu_do_on(FailPoints::slowdown_parallel_replicas_local_plan_read,
        {
            sleepForMilliseconds(20);
        });
        return coordinator->handleRequest(std::move(req));
    };

    auto read_from_merge_tree_parallel_replicas = reading->createLocalParallelReplicasReadingStep(
        context, analyzed_result_ptr, std::move(all_ranges_cb), std::move(read_task_cb), replica_number);
    node->step = std::move(read_from_merge_tree_parallel_replicas);

    addConvertingActions(*query_plan, header, context);

    return {std::move(query_plan), true};
}

}
