#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TopNAggregatingStep.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <climits>

namespace DB::QueryPlanOptimizations
{

/// Trace a column name backwards through an ActionsDAG to find its original input name.
/// Returns empty string if the column cannot be traced to a single input.
static String traceColumnThroughDAG(const ActionsDAG & dag, const String & column_name)
{
    const auto * node = dag.tryFindInOutputs(column_name);
    if (!node)
        return {};

    while (true)
    {
        if (node->type == ActionsDAG::ActionType::INPUT)
            return node->result_name;

        if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            if (node->children.size() != 1)
                return {};
            node = node->children[0];
            continue;
        }

        return {};
    }
}

/// Find the ReadFromMergeTree node below the given node (possibly through ExpressionStep).
static ReadFromMergeTree * findReadFromMergeTree(QueryPlan::Node & node)
{
    auto * current = &node;
    while (current)
    {
        if (auto * read = typeid_cast<ReadFromMergeTree *>(current->step.get()))
            return read;

        if (current->children.size() != 1)
            return nullptr;

        if (typeid_cast<ExpressionStep *>(current->step.get()))
        {
            current = current->children[0];
            continue;
        }

        return nullptr;
    }
    return nullptr;
}

void optimizeTopNAggregation(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings &)
{
    /// Pattern: LimitStep -> SortingStep -> [ExpressionStep] -> AggregatingStep -> ...
    auto * limit_step = typeid_cast<LimitStep *>(node.step.get());
    if (!limit_step)
        return;

    if (limit_step->getLimit() == 0 || limit_step->withTies())
        return;

    if (node.children.size() != 1)
        return;

    auto * sort_node = node.children[0];
    auto * sorting_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sorting_step)
        return;

    const auto & sort_desc = sorting_step->getSortDescription();
    if (sort_desc.empty())
        return;

    /// We only handle single sort column (the ORDER BY aggregate).
    if (sort_desc.size() != 1)
        return;

    if (sort_node->children.size() != 1)
        return;

    /// Look for optional ExpressionStep, then AggregatingStep.
    auto * after_sort_child = sort_node->children[0];
    ExpressionStep * expr_step = nullptr;
    QueryPlan::Node * expr_node = nullptr;
    QueryPlan::Node * agg_node = nullptr;

    if (auto * maybe_expr = typeid_cast<ExpressionStep *>(after_sort_child->step.get()))
    {
        expr_step = maybe_expr;
        expr_node = after_sort_child;

        if (expr_node->children.size() != 1)
            return;
        agg_node = expr_node->children[0];
    }
    else
    {
        agg_node = after_sort_child;
    }

    auto * aggregating_step = typeid_cast<AggregatingStep *>(agg_node->step.get());
    if (!aggregating_step)
        return;

    /// Disqualifying conditions.
    if (aggregating_step->isGroupingSets())
        return;
    if (aggregating_step->getParams().overflow_row)
        return;

    const auto & params = aggregating_step->getParams();
    if (params.keys.empty())
        return;

    const auto & agg_descs = params.aggregates;
    if (agg_descs.empty())
        return;

    /// Find which aggregate the ORDER BY column refers to.
    String sort_col_name = sort_desc[0].column_name;

    /// Trace through ExpressionStep if present.
    String original_sort_col_name = sort_col_name;
    if (expr_step)
    {
        auto traced = traceColumnThroughDAG(expr_step->getExpression(), sort_col_name);
        if (traced.empty())
            return;
        original_sort_col_name = traced;
    }

    /// Find the matching aggregate.
    size_t order_by_agg_idx = agg_descs.size();
    for (size_t i = 0; i < agg_descs.size(); ++i)
    {
        if (agg_descs[i].column_name == original_sort_col_name)
        {
            order_by_agg_idx = i;
            break;
        }
    }

    if (order_by_agg_idx >= agg_descs.size())
        return;

    /// Check that the ORDER BY aggregate supports TopK.
    const auto & order_agg = agg_descs[order_by_agg_idx];
    auto order_info = order_agg.function->getTopKAggregateInfo();
    if (order_info.determined_by_first_row_direction == 0)
        return;

    /// The ORDER BY direction must match the aggregate's determined_by_first_row_direction.
    /// For max(col) ORDER BY ... DESC: aggregate returns -1 (DESC), sort direction must be -1.
    /// For min(col) ORDER BY ... ASC: aggregate returns 1 (ASC), sort direction must be 1.
    int required_direction = order_info.determined_by_first_row_direction;
    int sort_direction = sort_desc[0].direction;

    /// INT_MAX means "any direction" (used by `any`).
    if (required_direction != INT_MAX && required_direction != sort_direction)
        return;

    if (required_direction == INT_MAX)
        required_direction = sort_direction;

    /// Check ALL aggregates support TopK with the same direction.
    for (const auto & agg : agg_descs)
    {
        auto info = agg.function->getTopKAggregateInfo();
        if (info.determined_by_first_row_direction == 0)
            return;
        if (info.determined_by_first_row_direction != INT_MAX && info.determined_by_first_row_direction != required_direction)
            return;
    }

    /// All checks passed. Build the TopNAggregatingStep.
    size_t limit_value = limit_step->getLimit();

    /// Build sort description using the original aggregate column name (pre-expression).
    SortDescription topn_sort_desc;
    topn_sort_desc.push_back(SortColumnDescription(original_sort_col_name, sort_direction));

    /// Check if we can do early termination (mode 1) by checking if ReadFromMergeTree exists
    /// and its sorting key matches the aggregate argument.
    bool sorted_input = false;
    ReadFromMergeTree * read_from_mt = nullptr;

    if (agg_node->children.size() == 1)
    {
        read_from_mt = findReadFromMergeTree(*agg_node->children[0]);
    }

    if (read_from_mt)
    {
        /// Check if the ORDER BY aggregate's argument matches a prefix of the table's sorting key.
        /// For max(col): argument is col. For argMax(val, col): argument is col (last argument).
        const auto & order_arg_name = order_agg.argument_names.back();

        /// Get the table's sorting key column names.
        const auto & sorting_key_columns = read_from_mt->getStorageMetadata()->getSortingKeyColumns();

        if (!sorting_key_columns.empty() && sorting_key_columns[0] == order_arg_name)
        {
            /// Request reading in order matching the aggregate direction.
            int read_direction = required_direction;
            if (read_from_mt->requestReadingInOrder(1, read_direction, limit_value))
                sorted_input = true;
        }
    }

    auto topn_step = std::make_unique<TopNAggregatingStep>(
        agg_node->step->getInputHeaders().front(),
        Names(params.keys.begin(), params.keys.end()),
        AggregateDescriptions(agg_descs.begin(), agg_descs.end()),
        topn_sort_desc,
        limit_value,
        sorted_input);

    /// Rewire the plan.
    if (expr_node)
    {
        /// Create a new node for the TopNAggregatingStep.
        auto & topn_node = nodes.emplace_back();
        topn_node.step = std::move(topn_step);
        topn_node.children = agg_node->children;

        /// Replace the LimitStep node with the ExpressionStep (keeping it for column renames).
        node.step = std::move(expr_node->step);
        node.children = {&topn_node};
    }
    else
    {
        /// Replace the LimitStep node directly with TopNAggregatingStep.
        node.step = std::move(topn_step);
        node.children = agg_node->children;
    }
}

}
