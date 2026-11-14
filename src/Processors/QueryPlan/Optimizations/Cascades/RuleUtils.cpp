#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StorageLayout.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/VectorWithMemoryTracking.h>
#include <fmt/format.h>
#include <algorithm>

namespace DB
{

bool isTopNSort(const IQueryPlanStep & step)
{
    const auto * sorting_step = typeid_cast<const SortingStep *>(&step);
    /// A partitioned bounded sort keeps one stream per hash partition instead of merging to
    /// a global top-N, so the top-N rules (which promise one sorted stream) must not take it.
    return sorting_step != nullptr && sorting_step->getType() == SortingStep::Type::Full
        && sorting_step->getLimit() > 0 && !sorting_step->hasPartitions();
}

bool isDistributionPassthroughStep(const IQueryPlanStep & step)
{
    /// A preliminary `DISTINCT` deduplicates each stream on its own, so it is correct on any split of its
    /// input and keeps the order within a stream.
    if (const auto * distinct = typeid_cast<const DistinctStep *>(&step))
        return distinct->isPreliminary();
    return typeid_cast<const ExpressionStep *>(&step) != nullptr
        || typeid_cast<const FilterStep *>(&step) != nullptr
        || typeid_cast<const BuildRuntimeFilterStep *>(&step) != nullptr;
}

GroupExpressionPtr tryMakeBucketedReadVariant(
    const GroupExpressionPtr & expression,
    size_t node_count,
    size_t target_buckets,
    ImplementationStrategyPtr strategy,
    const char * description_prefix,
    bool is_replicated,
    size_t & actual_buckets)
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto bucketed_read_step_ptr = cloneStepAs(*read_step);
    auto * bucketed_read_step = bucketed_read_step_ptr.get();

    actual_buckets = bucketed_read_step->setupDistributedReadBuckets(target_buckets, ReadFromMergeTree::max_distributed_read_buckets);
    if (actual_buckets != target_buckets)
        return nullptr;
    bucketed_read_step->setStepDescription(fmt::format("{} {}", description_prefix, read_step->getStepDescription()), 200);

    GroupExpressionPtr bucketed_read_expression = std::make_shared<GroupExpression>(*expression);
    bucketed_read_expression->plan_step = std::move(bucketed_read_step_ptr);
    bucketed_read_expression->strategy = std::move(strategy);

    ExpressionProperties output_properties;
    output_properties.distribution.node_count = node_count;
    output_properties.distribution.is_replicated = is_replicated;
    bucketed_read_expression->properties = output_properties;
    return bucketed_read_expression;
}

bool joinPreservesLeftSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

bool joinPreservesRightSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Right || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

/// The layouts that the result of a join can have: the layouts of each preserved input. In the rows of an
/// inner join the two columns of an equality are equal, so an argument column also gets the name of the
/// column on the other side. This lets the next join or an aggregation use the layout by either name.
static StorageLayouts getJoinStorageLayouts(
    const JoinStepLogical & join_step, const StorageLayouts & left_layouts, const StorageLayouts & right_layouts)
{
    const auto kind = join_step.getJoinOperator().kind;
    const auto & output_header = *join_step.getOutputHeader();

    /// Pairs of equal columns, only for an inner join: an outer join extends one side with defaults.
    VectorWithMemoryTracking<std::pair<String, String>> equal_columns;
    if (kind == JoinKind::Inner)
    {
        for (const auto & predicate : join_step.getJoinOperator().expression)
        {
            auto [op, left_node, right_node] = predicate.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                continue;
            if (left_node.fromRight() && right_node.fromLeft())
                std::swap(left_node, right_node);
            else if (!left_node.fromLeft() || !right_node.fromRight())
                continue;
            if (left_node.getType()->equals(*right_node.getType()))
                equal_columns.emplace_back(left_node.getColumnName(), right_node.getColumnName());
        }
    }

    StorageLayouts result;
    auto add_layouts_of_input = [&](const StorageLayouts & input_layouts)
    {
        for (const auto & input_layout : input_layouts)
        {
            StorageLayout layout;
            layout.function = input_layout.function;

            bool all_arguments_reach_output = true;
            for (const auto & input_argument : input_layout.argument_columns)
            {
                NameSet argument = input_argument;
                for (const auto & [left_column, right_column] : equal_columns)
                {
                    if (input_argument.contains(left_column))
                        argument.insert(right_column);
                    if (input_argument.contains(right_column))
                        argument.insert(left_column);
                }
                std::erase_if(argument, [&](const String & name) { return !output_header.has(name); });
                if (argument.empty())
                {
                    all_arguments_reach_output = false;
                    break;
                }
                layout.argument_columns.push_back(std::move(argument));
            }
            if (!all_arguments_reach_output)
                continue;

            /// The two inputs of a co-located join give the same layout.
            const bool is_known = std::ranges::any_of(result, [&](const StorageLayout & known)
            {
                return known.function->equals(*layout.function) && known.argument_columns == layout.argument_columns;
            });
            if (!is_known)
                result.push_back(std::move(layout));
        }
    };

    if (joinPreservesLeftSide(kind))
        add_layouts_of_input(left_layouts);
    if (joinPreservesRightSide(kind))
        add_layouts_of_input(right_layouts);
    return result;
}

const StorageLayouts & getDeliveredStorageLayouts(Memo & memo, GroupId group_id)
{
    auto group = memo.getGroup(group_id);
    if (group->storage_layouts)
        return *group->storage_layouts;

    StorageLayouts layouts;
    if (memo.getContext().distributed_plan_use_partition_key_layout && !group->logical_expressions.empty())
    {
        /// All logical expressions of a group give the same result, so the first one is enough.
        const auto & expression = group->logical_expressions.front();
        const IQueryPlanStep * step = expression->getQueryPlanStep();

        if (const auto * read_step = typeid_cast<const ReadFromMergeTree *>(step))
        {
            layouts = getStorageLayouts(read_step->getStorageMetadata()->getPartitionKey());
        }
        else if (isDistributionPassthroughStep(*step) && expression->inputs.size() == 1)
        {
            /// Such a step keeps every row on its node. It can rename the argument columns or drop them.
            const auto & input_layouts = getDeliveredStorageLayouts(memo, expression->inputs.front().group_id);
            if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
                layouts = remapStorageLayouts(input_layouts, expression_step->getExpression());
            else if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
                layouts = remapStorageLayouts(input_layouts, filter_step->getExpression());
            else
                layouts = input_layouts;
        }
        else if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && expression->inputs.size() == 2)
        {
            /// A candidate only: the join delivers the layout if it keeps the rows of that input on their nodes,
            /// which the search checks when a consumer requires the layout from this group.
            layouts = getJoinStorageLayouts(
                *join_step,
                getDeliveredStorageLayouts(memo, expression->inputs[0].group_id),
                getDeliveredStorageLayouts(memo, expression->inputs[1].group_id));
        }
    }

    group->storage_layouts = std::move(layouts);
    return *group->storage_layouts;
}

std::optional<size_t> findPartitionKeyComponent(const ReadFromMergeTree & read_step, const DistributionDescription & distribution)
{
    if (!distribution.layout_function)
        return std::nullopt;

    const auto & partition_key = read_step.getStorageMetadata()->getPartitionKey();
    for (size_t component_index = 0; component_index < partition_key.column_names.size(); ++component_index)
    {
        auto function = StorageLayoutFunction::tryCreate(partition_key, component_index);
        if (!function || !function->equals(*distribution.layout_function))
            continue;

        /// The required columns must be the argument columns of this table, in the order of the function.
        const auto & argument_names = function->getArgumentNames();
        if (argument_names.size() != distribution.columns.size())
            continue;
        bool arguments_match = true;
        for (size_t i = 0; i < argument_names.size() && arguments_match; ++i)
            arguments_match = distribution.columns[i].contains(argument_names[i]);
        if (arguments_match)
            return component_index;
    }
    return std::nullopt;
}

GroupExpressionPtr tryMakePartitionedReadVariant(
    const GroupExpressionPtr & expression, size_t component_index, const DistributionDescription & distribution)
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto partitioned_read_step = cloneStepAs(*read_step);
    if (partitioned_read_step->setupDistributedReadBucketsByPartitionValue(component_index, distribution.node_count) != distribution.node_count)
        return nullptr;
    partitioned_read_step->setStepDescription(fmt::format("PartitionedRead {}", read_step->getStepDescription()), 200);

    GroupExpressionPtr partitioned_read_expression = std::make_shared<GroupExpression>(*expression);
    partitioned_read_expression->plan_step = std::move(partitioned_read_step);
    partitioned_read_expression->strategy = strategySingleton<PartitionedReadStrategy>();
    partitioned_read_expression->properties = ExpressionProperties{};
    partitioned_read_expression->properties.distribution = distribution;
    return partitioned_read_expression;
}

GroupExpressionPtr makeEnforcerExpression(
    const GroupExpressionPtr & source,
    QueryPlanStepPtr step,
    ExpressionProperties input_required,
    ExpressionProperties output_properties,
    EnforcedProperty enforced)
{
    auto enforcer_expression = std::make_shared<GroupExpression>(std::move(step));
    enforcer_expression->group_id = source->group_id;
    enforcer_expression->inputs.push_back({.group_id = source->group_id, .required_properties = std::move(input_required)});
    enforcer_expression->properties = std::move(output_properties);
    enforcer_expression->enforced_property = enforced;
    return enforcer_expression;
}

}
