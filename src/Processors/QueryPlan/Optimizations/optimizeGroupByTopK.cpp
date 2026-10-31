#include <Interpreters/ActionsDAG.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/topKDynamicFilter.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <optional>

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

    if (aggregating_step->inOrder())
        return nullptr;

    /// Partial aggregation is annotated only by the shard-side Planner hook
    /// (`applyTopKPushdownToPartialAggregation`), which requires a real `ORDER BY` prefix.
    /// This pass must not match it: a deserialized shipped plan is re-optimized on the
    /// follower, and synthesizing a sort + limit over *partial* states would truncate each
    /// follower's group set independently, with no initiator-side sort to discard the
    /// resulting incomplete groups.
    if (!aggregating_step->isFinal())
        return nullptr;

    const auto & params = aggregating_step->getParams();

    if (params.top_k)
        return nullptr;

    if (params.only_merge)
        return nullptr;

    if (params.overflow_row)
        return nullptr;

    if (params.max_rows_to_group_by > 0)
        return nullptr;

    if (params.keys.empty())
        return nullptr;

    return aggregating_step;
}


/// Links the top-K heap of the aggregation to the `ReadFromMergeTree` step below it (see `tryAttachTopKDynamicFilter`).
///
/// The heap admits only keys not worse than its boundary once it holds `LIMIT` of them, so a row whose first ranked
/// key column is beyond the boundary can never reach the result.
static TopKThresholdTrackerPtr tryAttachDynamicFilter(
    QueryPlan::Node * aggregating_node,
    const AggregatingStep & aggregating_step,
    size_t limit,
    size_t num_key_columns,
    const std::vector<int> & directions,
    const std::vector<int> & nulls_directions,
    const Optimization::ExtraSettings & settings,
    QueryPlan::Nodes & nodes)
{
    if (!settings.enable_group_by_top_k_dynamic_filtering)
        return nullptr;

    if (aggregating_node->children.size() != 1)
        return nullptr;

    /// Unlike `ORDER BY`, the boundary on the first key column depends on every grouping key: the heap ranks groups,
    /// and the number of groups per value of the first key depends on the other keys (`GROUP BY a, b` and
    /// `GROUP BY a, c` reach different boundaries on `a` over the same rows). So the salt includes all the grouping
    /// keys with their types. Only the first key has to be passed through unchanged, the others may be computed
    /// between the read and the aggregation (`GROUP BY a, g` over `SELECT a, b % 100 AS g`).
    const auto & aggregation_keys = aggregating_step.getParams().keys;
    const auto & aggregation_input_header = *aggregating_step.getInputHeaders().front();

    TopKDynamicFilterConsumer consumer{
        .column_name = aggregation_keys.front(),
        .limit = limit,
        .num_ranked_columns = num_key_columns,
        .directions = directions,
        .nulls_directions = nulls_directions,
        .salt_mark = "group_by_top_k",
        .add_salt = [&](SipHash & hash)
        {
            hash.update(aggregation_keys.size());
            for (const auto & aggregation_key : aggregation_keys)
            {
                hash.update(aggregation_key);
                hash.update(aggregation_input_header.getByName(aggregation_key).type->getName());
            }
        },
    };

    return tryAttachTopKDynamicFilter(aggregating_node->children.front(), consumer, settings, nodes, getLogger("optimizeGroupByTopK"));
}

size_t tryOptimizeGroupByTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (!settings.enable_group_by_top_k_optimization)
        return 0;

    /// The distributed planner splits aggregation itself; shard-local heaps for that
    /// path are future work. `serialize_query_plan` is fine: `AggregatingStep::serialize`
    /// carries `top_k` since `Aggregating` step version 1, and omits it towards older
    /// followers (which then aggregate without the heap - the safe direction).
    if (settings.make_distributed_plan)
        return 0;

    /// The planner puts the heap on the partial aggregation of a distributed query itself
    /// (`applyTopKPushdownToPartialAggregation`); such a plan has no `LimitStep` above the aggregation,
    /// so only the link to the reading step is added here.
    if (auto * aggregating_step = typeid_cast<AggregatingStep *>(parent_node->step.get()))
    {
        const auto & params = aggregating_step->getParams();
        if (params.top_k && !params.top_k->threshold_tracker)
        {
            auto threshold_tracker = tryAttachDynamicFilter(
                parent_node,
                *aggregating_step,
                params.top_k->k,
                params.top_k->key_columns,
                params.top_k->directions,
                params.top_k->nulls_directions,
                settings,
                nodes);
            if (threshold_tracker)
                aggregating_step->setTopKThresholdTracker(std::move(threshold_tracker));
        }
        return 0;
    }

    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    if (limit_step->withTies())
        return 0;

    if (limit_step->alwaysReadTillEnd())
        return 0;

    size_t limit = limit_step->getLimitForSorting();
    if (limit < 1)
        return 0;

    if (settings.max_limit_for_top_k_optimization != 0 && limit > settings.max_limit_for_top_k_optimization)
        return 0;

    if (limit > Aggregator::Params::TopKParams::max_k)
    {
        LOG_DEBUG(
            getLogger("optimizeGroupByTopK"),
            "Skipping GROUP BY top-K optimization: the requested heap size {} is larger than the maximum {}",
            limit, Aggregator::Params::TopKParams::max_k);
        return 0;
    }

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

    QueryPlan::Node * node_above_aggregation = parent_node;
    const ExpressionStep * expression_step = typeid_cast<const ExpressionStep *>(next_node->step.get());
    if (expression_step)
    {
        if (next_node->children.size() != 1)
            return 0;

        /// An arrayJoin between the aggregation and the limit changes row
        /// multiplicity: it can produce zero rows for a group, so the smallest
        /// N groups no longer guarantee N output rows and pruning loses groups
        /// the limit still needs.
        if (expression_step->getExpression().hasArrayJoin())
            return 0;

        node_above_aggregation = next_node;
        next_node = next_node->children.front();
    }

    auto * aggregating_step = validateAggregatingStep(next_node);
    if (!aggregating_step)
        return 0;

    QueryPlan::Node * aggregating_node = next_node;
    const auto & params = aggregating_step->getParams();

    if (!settings.is_explain && params.stats_collecting_params.isCollectionAndUseEnabled())
    {
        const auto hint = getHashTablesStatistics<AggregationEntry>().getSizeHint(params.stats_collecting_params);
        if (hint && static_cast<Float64>(hint->sum_of_sizes) <= static_cast<Float64>(limit) * 1.5)
            return 0;
    }

    std::vector<int> directions;
    std::vector<int> nulls_directions;
    size_t num_key_columns = 0;

    if (sorting_step)
    {
        const auto & sort_description = sorting_step->getSortDescription();
        if (sort_description.empty())
            return 0;

        num_key_columns = std::min(sort_description.size(), params.keys.size());

        directions.reserve(num_key_columns);
        nulls_directions.reserve(num_key_columns);

        for (size_t i = 0; i < num_key_columns; ++i)
        {
            if (sort_description[i].column_name != params.keys[i])
                return 0;

            if (expression_step && !isSortKeyPassThrough(expression_step->getExpression(), params.keys[i]))
                return 0;

            if (sort_description[i].collator)
                return 0;

            directions.push_back(sort_description[i].direction);
            nulls_directions.push_back(sort_description[i].nulls_direction);
        }
    }
    else
    {
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

        chassert(blocksHaveEqualStructure(
            *sort_node.step->getOutputHeader(), *aggregating_node->step->getOutputHeader()));
    }

    const bool synthetic_sort = sorting_step == nullptr;

    Aggregator::Params::TopKParams top_k_params{
        .k = limit,
        .directions = std::move(directions),
        .nulls_directions = std::move(nulls_directions),
        .key_columns = num_key_columns,
        .observation_rows = synthetic_sort ? 0 : settings.top_k_optimization_observation_rows,
        .shared_boundary = settings.top_k_optimization_shared_boundary,
        .threshold_tracker = nullptr,
    };

    top_k_params.threshold_tracker = tryAttachDynamicFilter(
        aggregating_node,
        *aggregating_step,
        limit,
        num_key_columns,
        top_k_params.directions,
        top_k_params.nulls_directions,
        settings,
        nodes);

    aggregating_step->applyTopKOptimization(std::move(top_k_params));

    return 0;
}
}
