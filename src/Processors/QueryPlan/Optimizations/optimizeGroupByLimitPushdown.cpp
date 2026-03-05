#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>

namespace DB::QueryPlanOptimizations
{

/// Validates that the AggregatingStep is eligible for the top-N heap optimization.
/// Returns a pointer to the step if valid, nullptr otherwise.
static AggregatingStep * validateAggregatingStep(QueryPlan::Node * node)
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return nullptr;

    /// The optimization cannot be applied for non-final aggregation (e.g. distributed queries
    /// or parallel replicas) because each shard/replica would independently keep only its top-N
    /// keys and the merge step would produce wrong results.
    if (!aggregating_step->getFinal())
        return nullptr;

    if (aggregating_step->isGroupingSets())
        return nullptr;

    const auto & params = aggregating_step->getParams();

    /// WITH TOTALS uses overflow_row which is incompatible with key pruning.
    if (params.overflow_row)
        return nullptr;

    if (params.keys.empty())
        return nullptr;

    return aggregating_step;
}

/// Optimization for GROUP BY ... [ORDER BY ...] LIMIT queries.
///
/// Two patterns are supported:
///
/// Pattern 1: GROUP BY <keys> ORDER BY <prefix of keys> ASC LIMIT N
///   Plan: LimitStep -> SortingStep -> [ExpressionStep] -> AggregatingStep
///   The heap tracks ORDER BY columns (a leading prefix of GROUP BY keys).
///   Supports per-column collators from the ORDER BY clause.
///
/// Pattern 2: GROUP BY <keys> LIMIT N (no ORDER BY)
///   Plan: LimitStep -> [ExpressionStep] -> AggregatingStep
///   The heap tracks all GROUP BY keys using default ascending order.
///   Since the user did not request any particular order, any N groups
///   are a valid result. The heap retains groups with the smallest keys,
///   and the LimitStep produces the final N rows.
///
/// For both patterns, a bounded max-heap of size N is maintained during
/// aggregation to prune GROUP BY keys that won't appear in the result.
///
/// The ORDER BY columns (pattern 1) must be a leading prefix of the GROUP BY keys.
/// For example, GROUP BY x, y, z ORDER BY x, y LIMIT 10 is supported:
/// the heap tracks (x, y) pairs and skips rows where (x, y) is strictly
/// greater than the heap boundary.
///
/// Conditions:
///   - Pattern 1: ORDER BY columns are a prefix of GROUP BY keys, ASC only
///   - Final aggregation (not distributed partial)
///   - No GROUPING SETS
///   - No overflow row (WITH TOTALS)
///   - Not LIMIT WITH TIES
///   - Not exact_rows_before_limit mode (always_read_till_end)
size_t tryOptimizeGroupByLimitPushdown(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    if (!settings.ordered_group_by_limit_pushdown)
        return 0;

    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// LIMIT WITH TIES may produce more rows than the limit value,
    /// so we cannot safely prune keys.
    if (limit_step->withTies())
        return 0;

    /// When exact_rows_before_limit is set, we need to count all rows
    /// that would have been returned without the LIMIT, so pruning
    /// aggregation keys would produce a wrong count.
    if (limit_step->alwaysReadTillEnd())
        return 0;

    size_t limit = limit_step->getLimitForSorting();
    if (limit < 1)
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * child_node = parent_node->children.front();

    /// Pattern 1: LimitStep -> SortingStep -> [ExpressionStep] -> AggregatingStep
    if (auto * sorting_step = typeid_cast<SortingStep *>(child_node->step.get()))
    {
        if (sorting_step->getType() != SortingStep::Type::Full)
            return 0;

        if (child_node->children.size() != 1)
            return 0;

        /// Allow an optional ExpressionStep between SortingStep and AggregatingStep.
        /// The planner inserts "Before ORDER BY" expression steps there.
        auto * next_node = child_node->children.front();
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
        const auto & sort_description = sorting_step->getSortDescription();

        /// ORDER BY columns must be a prefix of GROUP BY keys (in order).
        if (sort_description.empty() || sort_description.size() > params.keys.size())
            return 0;

        std::vector<const Collator *> collators;
        collators.reserve(sort_description.size());

        for (size_t i = 0; i < sort_description.size(); ++i)
        {
            if (sort_description[i].column_name != params.keys[i])
                return 0;

            /// Currently only ascending sort order is supported.
            if (sort_description[i].direction != 1)
                return 0;

            collators.push_back(sort_description[i].collator ? sort_description[i].collator.get() : nullptr);
        }

        size_t num_key_columns = sort_description.size();
        LOG_DEBUG(getLogger("QueryPlanOptimizations"),
            "GROUP BY ... ORDER BY ... LIMIT optimization applied (top_n_keys={}, order_by_keys={}, group_by_keys={})",
            limit, num_key_columns, params.keys.size());
        aggregating_step->applyLimitPushdown(limit, std::move(collators), num_key_columns);
        return 0;
    }

    /// Pattern 2: LimitStep -> [ExpressionStep] -> AggregatingStep (no ORDER BY)
    /// When the user writes GROUP BY <keys> LIMIT N without ORDER BY,
    /// apply the heap optimization using all GROUP BY keys in their
    /// declaration order. Any N groups are a valid result since no
    /// particular order was requested.
    {
        auto * next_node = child_node;
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
        size_t num_key_columns = params.keys.size();

        /// No collators needed — there is no ORDER BY clause to specify collation.
        std::vector<const Collator *> collators(num_key_columns, nullptr);

        LOG_DEBUG(getLogger("QueryPlanOptimizations"),
            "GROUP BY ... LIMIT optimization applied (top_n_keys={}, group_by_keys={})",
            limit, num_key_columns);
        aggregating_step->applyLimitPushdown(limit, std::move(collators), num_key_columns);
        return 0;
    }
}

}
