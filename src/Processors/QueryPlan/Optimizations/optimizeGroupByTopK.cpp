#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

static bool isSortKeyPassThrough(const ActionsDAG & dag, const std::string & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    if (!node)
        return false;

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

    if (aggregating_step->inOrder())
        return nullptr;

    const auto & params = aggregating_step->getParams();

    if (params.top_k)
        return nullptr;

    if (params.overflow_row)
        return nullptr;

    if (params.max_rows_to_group_by > 0)
        return nullptr;

    if (params.keys.empty())
        return nullptr;

    return aggregating_step;
}

size_t tryOptimizeGroupByTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (!settings.enable_group_by_top_k_optimization)
        return 0;

    if (settings.make_distributed_plan)
        return 0;

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
    }

    aggregating_step->applyTopKOptimization(Aggregator::Params::TopKParams{
        .keys = limit,
        .directions = std::move(directions),
        .nulls_directions = std::move(nulls_directions),
        .key_columns = num_key_columns,
        .load_factor = settings.top_k_optimization_load_factor,
        .observation_rows = settings.top_k_optimization_observation_rows,
    });

    return 0;
}

}
