#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

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

/// Optimization for `GROUP BY ... [ORDER BY ...] LIMIT N` queries: maintain a
/// bounded heap of the top-N keys during aggregation and skip rows whose
/// grouping key cannot make it into the final result.
///
/// Two plan shapes are matched, differing only in the presence of a SortingStep:
///
/// Pattern 1: LimitStep -> SortingStep -> [ExpressionStep] -> AggregatingStep
///   `GROUP BY <keys> ORDER BY <prefix of keys> LIMIT N`.  The heap tracks the
///   ORDER BY columns (a leading prefix of the GROUP BY keys) with per-column
///   direction, NULLS direction, and collators.
///
/// Pattern 2: LimitStep -> [ExpressionStep] -> AggregatingStep
///   `GROUP BY <keys> LIMIT N` without ORDER BY.  Any N groups are a valid
///   result; the heap tracks all GROUP BY keys in default ascending order.
///   There is no downstream sort to rank stale partially-aggregated groups
///   below complete ones, so this pattern is only sound with hash-table
///   pruning — `requires_pruning` makes the aggregator disable the heap at
///   runtime for methods that cannot erase evicted keys.
///
/// Note on partial aggregation: the optimization is inherently safe for
/// Pattern 1 even if the matched `AggregatingStep` is partial (`final = false`)
/// — a key rejected by the heap has at least N better-ranked keys locally,
/// hence at least N better-ranked keys globally.  No plan tree currently
/// exposes such a pair to this optimizer; the parallel-replicas mirror in
/// `ParallelReplicasLocalPlan.cpp` applies the same logic from the AST.
size_t tryOptimizeGroupByLimitPushdown(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    if (!settings.enable_group_by_top_k_optimization)
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
    if (typeid_cast<ExpressionStep *>(next_node->step.get()))
    {
        if (next_node->children.size() != 1)
            return 0;
        next_node = next_node->children.front();
    }

    auto * aggregating_step = validateAggregatingStep(next_node);
    if (!aggregating_step)
        return 0;

    const auto & params = aggregating_step->getParams();

    std::vector<int> directions;
    std::vector<int> nulls_directions;
    std::vector<const Collator *> collators;
    size_t num_key_columns = 0;

    if (sorting_step)
    {
        /// ORDER BY columns must be a leading prefix of the GROUP BY keys (in order).
        const auto & sort_description = sorting_step->getSortDescription();
        if (sort_description.empty() || sort_description.size() > params.keys.size())
            return 0;

        directions.reserve(sort_description.size());
        nulls_directions.reserve(sort_description.size());
        collators.reserve(sort_description.size());

        bool has_collator = false;
        for (size_t i = 0; i < sort_description.size(); ++i)
        {
            if (sort_description[i].column_name != params.keys[i])
                return 0;

            directions.push_back(sort_description[i].direction);
            nulls_directions.push_back(sort_description[i].nulls_direction);
            collators.push_back(sort_description[i].collator ? sort_description[i].collator.get() : nullptr);
            if (sort_description[i].collator)
                has_collator = true;
        }

        /// Collators are not serialized through `AggregatingStep::serialize`, so
        /// when this partial aggregation may be shipped across the wire we could
        /// not reproduce the heap's ordering on the follower.
        if (has_collator && !aggregating_step->getFinal())
            return 0;

        num_key_columns = sort_description.size();
    }
    else
    {
        /// No explicit ORDER BY: default ascending order with NULLS LAST over all keys.
        num_key_columns = params.keys.size();
        directions.assign(num_key_columns, 1);
        nulls_directions.assign(num_key_columns, 1);
        collators.assign(num_key_columns, nullptr);
    }

    aggregating_step->applyLimitPushdown(
        limit,
        std::move(directions),
        std::move(nulls_directions),
        std::move(collators),
        num_key_columns,
        /*requires_pruning=*/ sorting_step == nullptr);
    return 0;
}

}
