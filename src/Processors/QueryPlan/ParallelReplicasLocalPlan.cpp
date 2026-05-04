#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>

#include <base/sleep.h>
#include <Common/checkStackSize.h>
#include <Common/FailPoint.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ConvertingActions.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
}

namespace FailPoints
{
    extern const char slowdown_parallel_replicas_local_plan_read[];
}

/// Finds and returns the first QueryPlan node containing the specified ReadingStep type or nullptr
template <class ReadingStep>
static QueryPlan::Node * findReadingStep(QueryPlan::Node * node)
{
    ReadingStep * reading_step = nullptr;
    while (node)
    {
        reading_step = typeid_cast<ReadingStep *>(node->step.get());
        if (reading_step)
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

    return node;
}

/// Walk the plan using the same traversal as findReadingStep (following LEFT/RIGHT JOIN logic),
/// but look for a UnionStep. If found, collect all ReadFromMergeTree steps from each child branch,
/// recursively handling nested views with their own UNION ALL.
static std::vector<QueryPlan::Node *> findReadingStepsUnderUnion(QueryPlan::Node * root, bool allow_view_over_mergetree)
{
    auto * node = root;
    while (node)
    {
        if (typeid_cast<const ReadFromMergeTree *>(node->step.get()))
        {
            /// Single reading step, not under a union — return it as a single-element vector.
            return {node};
        }

        /// A UnionStep that is NOT the plan root comes from a view expansion (e.g. UNION ALL view).
        /// If it IS the root, it's the outer query's UNION and should not be treated as a view.
        /// Only consider union steps when parallel_replicas_allow_view_over_mergetree is enabled.
        if (allow_view_over_mergetree && node != root && typeid_cast<UnionStep *>(node->step.get()))
        {
            /// Found a UnionStep from a view — recursively collect ReadFromMergeTree from each
            /// child branch. This handles nested views whose inner queries also contain UNION ALL.
            std::vector<QueryPlan::Node *> result;
            for (auto * child : node->children)
            {
                auto child_results = findReadingStepsUnderUnion(child, allow_view_over_mergetree);
                result.insert(result.end(), child_results.begin(), child_results.end());
            }
            return result;
        }

        if (!node->children.empty())
        {
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

    return {};
}

std::shared_ptr<const QueryPlan> createRemotePlanForParallelReplicas(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage)
{
    checkStackSize();

    auto new_context = Context::createCopy(context);

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage).ignoreASTOptimizations();
    select_query_options.build_logical_plan = true;

    /// For Analyzer, identifier in GROUP BY/ORDER BY/LIMIT BY lists has been resolved to
    /// ConstantNode in QueryTree if it is an alias of a constant, so we should not replace
    /// ConstantNode with ProjectionNode again(https://github.com/ClickHouse/ClickHouse/issues/62289).
    new_context->setSetting("enable_positional_arguments", Field(false));
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
    auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
    auto query_plan = std::make_shared<QueryPlan>(std::move(interpreter).extractQueryPlan());
    addConvertingActions(*query_plan, header, context);

    auto * node = findReadingStep<ReadFromTableStep>(query_plan->getRootNode());
    if (node)
        typeid_cast<ReadFromTableStep*>(node->step.get())->useParallelReplicas() = true;

    return query_plan;
}

std::pair<QueryPlanPtr, bool> createLocalPlanForParallelReplicas(
    const QueryTreeNodePtr & query_tree,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr analyzed_read_from_merge_tree,
    size_t replica_number)
{
    checkStackSize();

    /// Do not push down limit to local plan, as it will break `rows_before_limit_at_least` counter.
    if (processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit)
        processed_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;

    /// Since we're passing a pre-analyzed query tree (not AST), the interpreter won't run
    /// query tree passes anyway. We must NOT set ignoreASTOptimizations() here because it
    /// causes isASTLevelOptimizationAllowed() to return false in PlannerContext, which changes
    /// how constant node names are generated (using source expression instead of _CAST wrapper),
    /// leading to column name mismatches with the expected header.
    auto select_query_options = SelectQueryOptions(processed_stage);

    /// For Analyzer, identifier in GROUP BY/ORDER BY/LIMIT BY lists has been resolved to
    /// ConstantNode in QueryTree if it is an alias of a constant, so we should not replace
    /// ConstantNode with ProjectionNode again(https://github.com/ClickHouse/ClickHouse/issues/62289).
    auto new_context = Context::createCopy(context);
    new_context->setSetting("enable_positional_arguments", Field(false));
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));

    /// Clone the query tree and disable parallel replicas in ALL QueryNode/UnionNode contexts.
    /// Each node gets a copy of its own context with parallel replicas disabled.
    /// This is necessary because the Planner extracts the context from each QueryNode,
    /// and the original query_tree has contexts with parallel replicas enabled.
    /// Without updating all nodes, nested subqueries (e.g. in JOINs) would still have
    /// parallel replicas enabled in their contexts, causing the Planner to create
    /// additional `ParallelReplicasReadingCoordinator` instances.
    auto local_query_tree = query_tree->clone();
    {
        std::vector<IQueryTreeNode *> nodes_to_visit;
        nodes_to_visit.push_back(local_query_tree.get());
        while (!nodes_to_visit.empty())
        {
            auto * current = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * query_node = current->as<QueryNode>())
            {
                auto node_context = Context::createCopy(query_node->getContext());
                node_context->setSetting("enable_positional_arguments", Field(false));
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                query_node->getMutableContext() = std::move(node_context);
            }
            else if (auto * union_node = current->as<UnionNode>())
            {
                auto node_context = Context::createCopy(union_node->getContext());
                node_context->setSetting("enable_positional_arguments", Field(false));
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                union_node->getMutableContext() = std::move(node_context);
            }

            for (auto & child : current->getChildren())
            {
                if (child)
                    nodes_to_visit.push_back(child.get());
            }
        }
    }

    auto interpreter = InterpreterSelectQueryAnalyzer(local_query_tree, new_context, select_query_options);
    auto query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());

    const bool allow_view_over_mergetree = context->getSettingsRef()[Setting::parallel_replicas_allow_view_over_mergetree];
    auto reading_nodes = findReadingStepsUnderUnion(query_plan->getRootNode(), allow_view_over_mergetree);
    if (reading_nodes.empty())
    {
        /// it can happen if merge tree table is empty — it'll be replaced with ReadFromPreparedSource
        return {std::move(query_plan), false};
    }

    /// For the first reading step, reuse the pre-analyzed result if available.
    ReadFromMergeTree::AnalysisResultPtr analyzed_result_ptr;
    if (analyzed_read_from_merge_tree.get())
    {
        auto * analyzed_merge_tree = typeid_cast<ReadFromMergeTree *>(analyzed_read_from_merge_tree.get());
        if (analyzed_merge_tree)
            analyzed_result_ptr = analyzed_merge_tree->getAnalyzedResult();
    }

    for (auto * reading_node : reading_nodes)
    {
        auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());

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
        reading_node->step = std::move(read_from_merge_tree_parallel_replicas);

        /// Only the first reading step can reuse the pre-analyzed result.
        analyzed_result_ptr = nullptr;
    }

    addConvertingActions(*query_plan, header, context);

    return {std::move(query_plan), true};
}

}
