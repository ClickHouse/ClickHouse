#include <Interpreters/ActionsDAG.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <unordered_map>

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

        chassert(blocksHaveEqualStructure(
            *sort_node.step->getOutputHeader(), *aggregating_node->step->getOutputHeader()));
    }

    const bool synthetic_sort = sorting_step == nullptr;

    aggregating_step->applyTopKOptimization(Aggregator::Params::TopKParams{
        .keys = limit,
        .directions = std::move(directions),
        .nulls_directions = std::move(nulls_directions),
        .key_columns = num_key_columns,
        .load_factor = settings.top_k_optimization_load_factor,
        .observation_rows = synthetic_sort ? 0 : settings.top_k_optimization_observation_rows,
        .synthetic_sort = synthetic_sort,
    });

    return 0;
}

bool removeSyntheticTopKSort(QueryPlan::Node * aggregating_node, QueryPlan::Node * sort_node, QueryPlan::Node * parent_of_sort)
{
    if (!sort_node || !parent_of_sort)
        return false;

    if (!typeid_cast<SortingStep *>(sort_node->step.get())
        || sort_node->children.size() != 1
        || sort_node->children.front() != aggregating_node)
        return false;

    bool unlinked = false;
    for (auto & child : parent_of_sort->children)
    {
        if (child == sort_node)
        {
            child = aggregating_node;
            unlinked = true;
        }
    }
    return unlinked;
}

void abandonGroupByTopKForProjections(QueryPlan::Node & root)
{
    std::unordered_map<const QueryPlan::Node *, QueryPlan::Node *> parents;
    std::vector<QueryPlan::Node *> stack;
    stack.push_back(&root);

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        for (auto * child : node->children)
        {
            parents[child] = node;
            stack.push_back(child);
        }

        bool synthetic_sort = false;
        auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
        auto * aggregating_projection_step = typeid_cast<AggregatingProjectionStep *>(node->step.get());

        if (aggregating_step)
        {
            const auto & params = aggregating_step->getParams();
            if (!params.top_k || !params.only_merge)
                continue;
            synthetic_sort = params.top_k->synthetic_sort;
        }
        else if (aggregating_projection_step)
        {
            const auto & params = aggregating_projection_step->getParams();
            if (!params.top_k)
                continue;
            synthetic_sort = params.top_k->synthetic_sort;
        }
        else
            continue;

        if (synthetic_sort)
        {
            auto parent_it = parents.find(node);
            QueryPlan::Node * sort_node = parent_it == parents.end() ? nullptr : parent_it->second;
            auto grandparent_it = sort_node ? parents.find(sort_node) : parents.end();
            QueryPlan::Node * parent_of_sort = grandparent_it == parents.end() ? nullptr : grandparent_it->second;

            /// Even if the sort cannot be unlinked, abandoning the heap keeps the plan correct:
            /// the synthesized sort merely orders the result by the grouping keys before the limit.
            removeSyntheticTopKSort(node, sort_node, parent_of_sort);
        }

        if (aggregating_step)
            aggregating_step->abandonTopKOptimization();
        else
            aggregating_projection_step->abandonTopKOptimization();
    }
}

void abandonUnprofitableGroupByTopK(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    if (optimization_settings.is_explain)
        return;

    std::unordered_map<const QueryPlan::Node *, QueryPlan::Node *> parents;
    std::vector<QueryPlan::Node *> stack;
    stack.push_back(&root);

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        for (auto * child : node->children)
        {
            parents[child] = node;
            stack.push_back(child);
        }

        auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
        if (!aggregating_step)
            continue;

        const auto & params = aggregating_step->getParams();
        if (!params.top_k || !params.stats_collecting_params.isCollectionAndUseEnabled())
            continue;

        const auto hint = getHashTablesStatistics<AggregationEntry>().getSizeHint(params.stats_collecting_params);
        if (!hint
            || static_cast<Float64>(hint->sum_of_sizes)
                > static_cast<Float64>(params.top_k->keys) * params.top_k->load_factor)
            continue;

        if (params.top_k->synthetic_sort)
        {
            auto parent_it = parents.find(node);
            QueryPlan::Node * sort_node = parent_it == parents.end() ? nullptr : parent_it->second;
            auto grandparent_it = sort_node ? parents.find(sort_node) : parents.end();
            QueryPlan::Node * parent_of_sort = grandparent_it == parents.end() ? nullptr : grandparent_it->second;

            if (!removeSyntheticTopKSort(node, sort_node, parent_of_sort))
                continue;
        }

        aggregating_step->abandonTopKOptimization();
    }
}

}
