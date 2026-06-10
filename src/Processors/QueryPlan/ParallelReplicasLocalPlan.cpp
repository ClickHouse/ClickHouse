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
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/QueryPlan/AggregatingStep.h>
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
    extern const SettingsBool enable_group_by_top_k_optimization;
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

/// Recursively look for an `AggregatingStep` in the plan.
static AggregatingStep * findAggregatingStep(QueryPlan::Node * node)
{
    if (!node)
        return nullptr;
    if (auto * step = typeid_cast<AggregatingStep *>(node->step.get()))
        return step;
    for (auto * child : node->children)
    {
        if (auto * found = findAggregatingStep(child))
            return found;
    }
    return nullptr;
}

/// Find the inner `ASTSelectQuery` in a `query_ast`, unwrapping
/// `ASTSelectWithUnionQuery` if present.  Returns nullptr on anything else.
static const ASTSelectQuery * findInnerSelect(const ASTPtr & query_ast)
{
    if (auto * select = query_ast->as<ASTSelectQuery>())
        return select;
    if (auto * with_union = query_ast->as<ASTSelectWithUnionQuery>())
    {
        if (with_union->list_of_selects && !with_union->list_of_selects->children.empty())
            return with_union->list_of_selects->children.front()->as<ASTSelectQuery>();
    }
    return nullptr;
}

/// Apply the GROUP BY top-K pushdown to the partial `AggregatingStep` inside
/// the remote plan, mirroring the validation performed by
/// `tryOptimizeGroupByLimitPushdown` but against the AST (since the remote
/// plan, built at stage `WithMergeableState`, has no LimitStep / SortingStep
/// to drive the existing optimization).
///
/// Only Pattern 1 (ORDER BY = leading prefix of GROUP BY, simple identifiers,
/// no collators, no LIMIT WITH TIES, no WITH TOTALS/ROLLUP/CUBE) is handled
/// here.  Pattern 2 (no ORDER BY) is intentionally skipped because the
/// coordinator's `LimitStep` has no ordering with which to discard tuples
/// with corrupted partial state - extending it requires either a sort before
/// the final limit or a heap-aware merging step.
/// Evaluate `node` to a constant `UInt64`.  Handles the `_CAST(N, 'UInt64')`
/// wrapper that the planner injects around LIMIT literals.  Returns
/// nullopt when the expression isn't constant or doesn't fit a `UInt64`.
static std::optional<UInt64> evalConstantUInt64(const ASTPtr & node, const ContextPtr & context)
{
    if (!node)
        return std::nullopt;
    if (const auto * lit = node->as<ASTLiteral>())
    {
        if (lit->value.getType() != Field::Types::UInt64)
            return std::nullopt;
        return lit->value.safeGet<UInt64>();
    }
    auto evaluated = tryEvaluateConstantExpression(node, context);
    if (!evaluated)
        return std::nullopt;
    const Field & value = evaluated->first;
    if (value.getType() != Field::Types::UInt64)
        return std::nullopt;
    return value.safeGet<UInt64>();
}

static void tryPushDownTopKToPartialAggregation(QueryPlan & remote_plan, const ASTPtr & query_ast, const ContextPtr & context)
{
    const auto * select = findInnerSelect(query_ast);
    if (!select)
        return;

    /// Disqualifiers that match `validateAggregatingStep` /
    /// `tryOptimizeGroupByLimitPushdown` on the coordinator side.
    if (select->group_by_with_totals || select->group_by_with_rollup
        || select->group_by_with_cube || select->group_by_with_grouping_sets)
        return;
    if (select->limit_with_ties)
        return;
    if (select->limitBy() || select->limitByOffset())
        return;

    auto limit_opt = evalConstantUInt64(select->limitLength(), context);
    if (!limit_opt || *limit_opt == 0)
        return;
    UInt64 limit = *limit_opt;
    if (select->limitOffset())
    {
        auto offset_opt = evalConstantUInt64(select->limitOffset(), context);
        if (!offset_opt)
            return;
        const UInt64 offset = *offset_opt;
        if (offset > std::numeric_limits<UInt64>::max() - limit)
            return;
        limit += offset;
    }

    auto order_by_list = select->orderBy();
    auto group_by_list = select->groupBy();
    if (!order_by_list || !group_by_list)
        return;
    if (order_by_list->children.empty() || group_by_list->children.empty())
        return;
    if (order_by_list->children.size() > group_by_list->children.size())
        return;

    auto * agg_step = findAggregatingStep(remote_plan.getRootNode());
    if (!agg_step)
        return;
    if (agg_step->getFinal())
        return;
    if (agg_step->isGroupingSets())
        return;
    const auto & params = agg_step->getParams();
    if (params.overflow_row || params.max_rows_to_group_by > 0 || params.keys.empty())
        return;
    if (order_by_list->children.size() > params.keys.size())
        return;

    std::vector<int> directions;
    std::vector<int> nulls_directions;
    std::vector<const Collator *> collators;
    directions.reserve(order_by_list->children.size());
    nulls_directions.reserve(order_by_list->children.size());
    collators.reserve(order_by_list->children.size());

    /// The analyzer rewrites GROUP BY/ORDER BY identifiers into qualified
    /// names like `__table1.k`, so `params.keys` may not match the bare
    /// identifier from the AST.  Accept either an exact match or a "trailing
    /// segment" match (`<qualified>.endsWith("." + ident)`).  This covers the
    /// simple-identifier Pattern 1 cases without us having to plumb the
    /// QueryNode all the way down here.
    auto matches_key = [](std::string_view key, std::string_view ident_name) {
        if (key == ident_name)
            return true;
        if (key.size() > ident_name.size() + 1
            && key.ends_with(ident_name)
            && key[key.size() - ident_name.size() - 1] == '.')
            return true;
        return false;
    };

    for (size_t i = 0; i < order_by_list->children.size(); ++i)
    {
        const auto * ob = order_by_list->children[i]->as<ASTOrderByElement>();
        if (!ob)
            return;
        if (ob->with_fill)
            return;
        /// Skip when a collator is in play: AggregatingStep::serialize does not
        /// round-trip collators, so we cannot reproduce the heap's ordering on
        /// the follower.  See `optimizeGroupByLimitPushdown.cpp`.
        if (ob->getCollation())
            return;

        if (ob->children.empty())
            return;
        const auto * ident = ob->children.front()->as<ASTIdentifier>();
        if (!ident)
            return;
        if (!matches_key(params.keys[i], ident->name()))
            return;

        directions.push_back(ob->direction);
        nulls_directions.push_back(ob->nulls_direction ? ob->nulls_direction : ob->direction);
        collators.push_back(nullptr);
    }

    const size_t num_key_columns = order_by_list->children.size();
    agg_step->applyLimitPushdown(
        limit, std::move(directions), std::move(nulls_directions), std::move(collators), num_key_columns);
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

    /// Positional arguments in the outer query were already resolved by the initiator.
    /// Use a context flag instead of disabling enable_positional_arguments so that
    /// view-inner queries on this node are still processed correctly.
    /// See https://github.com/ClickHouse/ClickHouse/issues/62289.
    new_context->setPositionalArgumentsAlreadyResolved(true);
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
    auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
    auto query_plan = std::make_shared<QueryPlan>(std::move(interpreter).extractQueryPlan());
    addConvertingActions(*query_plan, header, context);

    auto * node = findReadingStep<ReadFromTableStep>(query_plan->getRootNode());
    if (node)
        typeid_cast<ReadFromTableStep*>(node->step.get())->useParallelReplicas() = true;

    /// If the user enabled the top-K heap optimization on the initiator, try
    /// to push it down to the partial aggregation that this remote plan
    /// represents.  The check covers the same conditions as the coordinator's
    /// `tryOptimizeGroupByLimitPushdown`; it intentionally skips collator and
    /// composite-expression cases that we do not yet serialize.
    if (new_context->getSettingsRef()[Setting::enable_group_by_top_k_optimization])
        tryPushDownTopKToPartialAggregation(*query_plan, query_ast, new_context);

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

    /// Positional arguments in the outer query were already resolved by the initiator.
    /// Use a context flag instead of disabling enable_positional_arguments so that
    /// view-inner queries on this node are still processed correctly.
    /// See https://github.com/ClickHouse/ClickHouse/issues/62289.
    auto new_context = Context::createCopy(context);
    new_context->setPositionalArgumentsAlreadyResolved(true);
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
                node_context->setPositionalArgumentsAlreadyResolved(true);
                node_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                query_node->getMutableContext() = std::move(node_context);
            }
            else if (auto * union_node = current->as<UnionNode>())
            {
                auto node_context = Context::createCopy(union_node->getContext());
                node_context->setPositionalArgumentsAlreadyResolved(true);
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
