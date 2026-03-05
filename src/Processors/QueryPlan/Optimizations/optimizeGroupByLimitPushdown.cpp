#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>

namespace DB::QueryPlanOptimizations
{

/// Optimization for GROUP BY ... ORDER BY ... LIMIT queries.
///
/// When the query has the pattern:
///   SELECT ... GROUP BY <keys> ORDER BY <prefix of keys> ASC LIMIT N
/// we can maintain a bounded max-heap of size N during aggregation,
/// pruning GROUP BY keys whose ORDER BY prefix is worse than the current
/// top-N boundary.
///
/// The ORDER BY columns must be a prefix of the GROUP BY keys.
/// For example, GROUP BY x, y, z ORDER BY x, y LIMIT 10 is supported:
/// the heap tracks (x, y) pairs and skips rows where (x, y) is strictly
/// greater than the heap boundary. Rows with equal (x, y) prefix are
/// kept regardless of z, which is correct because the final ORDER BY + LIMIT
/// will select among them.
///
/// Supports both single-column and composite (multi-column) ORDER BY prefixes.
/// For composite keys, the heap performs lexicographic comparison with
/// per-column collators.
///
/// Pattern matched in the query plan (top to bottom):
///   LimitStep -> SortingStep -> [ExpressionStep] -> AggregatingStep
///
/// Conditions:
///   - ORDER BY columns are a prefix of GROUP BY keys (in order)
///   - ASC sort order only (DESC not yet supported)
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

    auto * sorting_node = parent_node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting_step)
        return 0;

    if (sorting_step->getType() != SortingStep::Type::Full)
        return 0;

    if (sorting_node->children.size() != 1)
        return 0;

    /// Allow an optional ExpressionStep between SortingStep and AggregatingStep.
    /// The planner inserts "Before ORDER BY" expression steps there.
    auto * next_node = sorting_node->children.front();
    if (typeid_cast<ExpressionStep *>(next_node->step.get()))
    {
        if (next_node->children.size() != 1)
            return 0;
        next_node = next_node->children.front();
    }

    auto * aggregating_step = typeid_cast<AggregatingStep *>(next_node->step.get());
    if (!aggregating_step)
        return 0;

    /// The optimization cannot be applied for non-final aggregation (e.g. distributed queries
    /// or parallel replicas) because each shard/replica would independently keep only its top-N
    /// keys and the merge step would produce wrong results.
    if (!aggregating_step->getFinal())
        return 0;

    if (aggregating_step->isGroupingSets())
        return 0;

    const auto & params = aggregating_step->getParams();

    /// WITH TOTALS uses overflow_row which is incompatible with key pruning.
    if (params.overflow_row)
        return 0;

    if (params.keys.empty())
        return 0;

    const auto & sort_description = sorting_step->getSortDescription();

    /// ORDER BY columns must be a prefix of GROUP BY keys (in order).
    /// E.g. GROUP BY x, y, z ORDER BY x, y is valid; ORDER BY y, x is not.
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

}
