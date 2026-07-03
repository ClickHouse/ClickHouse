#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

/// True when `dag` emits `name` as an unchanged pass-through of its same-named
/// input (an `INPUT` node, possibly behind `ALIAS` renames that preserve the
/// name).  The heap ranks by the GROUP BY key column directly, so it is only
/// sound to match an `ORDER BY` key against a GROUP BY key by name if the
/// optional `ExpressionStep` between the sort and the aggregation does not
/// rewrite that column: otherwise the sort could order by `f(key)` while the
/// heap ranks by `key` (e.g. a `-k AS k` projection), keeping the wrong rows.
static bool isSortKeyPassThrough(const ActionsDAG & dag, const std::string & name)
{
    const auto & outputs = dag.getOutputs();
    auto it = std::find_if(
        outputs.begin(), outputs.end(), [&](const auto * node) { return node->result_name == name; });
    if (it == outputs.end())
        return false;

    const ActionsDAG::Node * node = *it;
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node->type == ActionsDAG::ActionType::INPUT && node->result_name == name;
}

/// Returns the AggregatingStep if it is eligible for the top-K heap optimization.
static AggregatingStep * validateAggregatingStep(QueryPlan::Node * node)
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return nullptr;

    if (aggregating_step->isGroupingSets())
        return nullptr;

    const auto & params = aggregating_step->getParams();

    /// WITH TOTALS uses overflow_row which is incompatible with key pruning.
    if (params.overflow_row)
        return nullptr;

    /// When max_rows_to_group_by is set, the aggregation already limits groups
    /// (via any/throw overflow mode). The heap optimization would interfere
    /// by changing which groups survive.
    if (params.max_rows_to_group_by > 0)
        return nullptr;

    if (params.keys.empty())
        return nullptr;

    return aggregating_step;
}

/// `GROUP BY ... [ORDER BY <prefix of keys>] LIMIT N`: maintain a bounded heap
/// of the top-N keys during aggregation and skip rows whose key cannot reach
/// the result.  Matches `LimitStep -> SortingStep -> [ExpressionStep] ->
/// AggregatingStep`.  A query without `ORDER BY` is promoted into that shape
/// by synthesizing a `SortingStep` over all keys (any N groups are a valid
/// answer, so any deterministic order works); the sort also discards any group
/// a heap eviction left partially aggregated, which is what makes the heap
/// sound in both cases.  Partial (shard-side) aggregation is handled from the
/// query tree in `Planner.cpp` instead, since its plan has no LimitStep.
size_t tryOptimizeGroupByLimitPushdown(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (!settings.enable_group_by_top_k_optimization)
        return 0;

    /// The distributed planner would split the annotated aggregation into
    /// independent partial aggregators.
    if (settings.make_distributed_plan)
        return 0;

    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// LIMIT WITH TIES may produce more rows than the limit value.
    if (limit_step->withTies())
        return 0;

    /// exact_rows_before_limit promises the count of all rows that would have
    /// been returned without the LIMIT; pruning groups would undercount it.
    if (limit_step->alwaysReadTillEnd())
        return 0;

    size_t limit = limit_step->getLimitForSorting();
    if (limit < 1)
        return 0;

    /// An unselective limit makes the heap pure overhead (0 = no cap).
    if (settings.max_limit_for_top_k_optimization != 0 && limit > settings.max_limit_for_top_k_optimization)
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * next_node = parent_node->children.front();

    auto * sorting_step = typeid_cast<SortingStep *>(next_node->step.get());
    if (sorting_step)
    {
        if (sorting_step->getType() != SortingStep::Type::Full)
            return 0;
        if (next_node->children.size() != 1)
            return 0;
        next_node = next_node->children.front();
    }

    /// Allow an optional ExpressionStep ("Before ORDER BY" / projection).
    QueryPlan::Node * node_above_aggregation = parent_node;
    const ExpressionStep * expression_step = typeid_cast<const ExpressionStep *>(next_node->step.get());
    if (expression_step)
    {
        if (next_node->children.size() != 1)
            return 0;
        node_above_aggregation = next_node;
        next_node = next_node->children.front();
    }

    auto * aggregating_step = validateAggregatingStep(next_node);
    if (!aggregating_step)
        return 0;

    QueryPlan::Node * aggregating_node = next_node;
    const auto & params = aggregating_step->getParams();

    std::vector<int> directions;
    std::vector<int> nulls_directions;
    size_t num_key_columns = 0;

    if (sorting_step)
    {
        /// ORDER BY columns must be a leading prefix of the GROUP BY keys (in order).
        const auto & sort_description = sorting_step->getSortDescription();
        if (sort_description.empty() || sort_description.size() > params.keys.size())
            return 0;

        directions.reserve(sort_description.size());
        nulls_directions.reserve(sort_description.size());

        for (size_t i = 0; i < sort_description.size(); ++i)
        {
            if (sort_description[i].column_name != params.keys[i])
                return 0;

            if (expression_step && !isSortKeyPassThrough(expression_step->getExpression(), params.keys[i]))
                return 0;

            /// The heap compares with `IColumn::compareAt`, which ignores collation.
            if (sort_description[i].collator)
                return 0;

            directions.push_back(sort_description[i].direction);
            nulls_directions.push_back(sort_description[i].nulls_direction);
        }

        num_key_columns = sort_description.size();
    }
    else
    {
        /// No ORDER BY: synthesize the sort directly above the aggregation
        /// (below any projection, so key names need no pass-through check).
        num_key_columns = params.keys.size();
        directions.assign(num_key_columns, 1);
        nulls_directions.assign(num_key_columns, 1);

        SortDescription sort_description;
        sort_description.reserve(num_key_columns);
        for (const auto & key : params.keys)
            sort_description.emplace_back(key, /*direction=*/ 1, /*nulls_direction=*/ 1);

        auto synthesized_sort = std::make_unique<SortingStep>(
            aggregating_node->step->getOutputHeader(),
            std::move(sort_description),
            limit,
            SortingStep::Settings(settings.max_block_size));
        synthesized_sort->setStepDescription("Sorting for GROUP BY top-K", settings.max_step_description_length);

        auto & sort_node = nodes.emplace_back();
        sort_node.step = std::move(synthesized_sort);
        sort_node.children = {aggregating_node};
        chassert(node_above_aggregation->children.front() == aggregating_node);
        node_above_aggregation->children.front() = &sort_node;
    }

    aggregating_step->applyLimitPushdown(
        limit,
        std::move(directions),
        std::move(nulls_directions),
        num_key_columns);
    return 0;
}

}
