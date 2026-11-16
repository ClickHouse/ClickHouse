#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <fmt/format.h>

namespace DB
{

/// The partition keys as distribution columns, or empty when the window cannot run per
/// node: without `PARTITION BY` all rows form one partition, and only a single node keeps
/// them together (empty distribution columns mean "any distribution"); a partition key
/// that is not a column of the input header cannot be hashed by name; and a key whose
/// hash disagrees with `compareAt` (floats, `JSON`, `Dynamic`: `-0.` and `0.` compare as
/// one partition but hash differently) would split one logical partition across nodes
/// and produce wrong window values.
DistributionColumns windowPartitionKeyColumns(const WindowStep & window_step)
{
    const auto & partition_by = window_step.getWindowDescription().partition_by;
    if (partition_by.empty())
        return {};

    const auto & input_header = window_step.getInputHeaders().at(0);
    DistributionColumns columns;
    for (const auto & partition_column : partition_by)
    {
        if (!input_header->has(partition_column.column_name))
            return {};
        if (QueryPlanOptimizations::keyTypeBreaksHashSharding(*input_header->getByName(partition_column.column_name).type))
            return {};
        columns.push_back(NameSet{partition_column.column_name});
    }
    return columns;
}

bool allowsHashFamilyAlgorithm(const std::vector<JoinAlgorithm> & join_algorithms)
{
    return TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH)
        || TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::GRACE_HASH)
        || TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::AUTO);
}

bool joinPreservesLeftSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

bool joinPreservesRightSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Right || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

std::vector<RoutingEquiKey> extractRoutingEquiKeys(const JoinStepLogical & join_step)
{
    std::vector<RoutingEquiKey> keys;
    for (const auto & predicate : join_step.getJoinOperator().expression)
    {
        /// The operands are empty for a conjunct that is not a binary predicate, such as a constant.
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;

        if (left_node.fromRight() && right_node.fromLeft())
            std::swap(left_node, right_node);
        else if (!left_node.fromLeft() || !right_node.fromRight())
            continue;

        DataTypePtr hash_type;
        if (!left_node.getType()->equals(*right_node.getType()))
        {
            hash_type = tryGetLeastSupertype(DataTypes{left_node.getType(), right_node.getType()});
            if (!hash_type)
                continue;
        }
        keys.push_back({std::move(left_node), std::move(right_node), std::move(hash_type)});
    }
    return keys;
}

bool isJoinDistributable(const JoinStepLogical & join_step)
{
    if (!allowsHashFamilyAlgorithm(join_step.getJoinSettings().join_algorithms))
        return false;
    return joinPreservesLeftSide(join_step.getJoinOperator().kind) || !extractRoutingEquiKeys(join_step).empty();
}

/// A `DISTINCT` that keeps the order of its input stays on one node: the shares of the nodes would interleave when
/// gathered. So does one with `max_rows_in_distinct` or `max_bytes_in_distinct`: each node would check the limit on
/// its own share only, and a result over the limit would pass.
bool isFinalDistinctDistributable(const DistinctStep & step)
{
    const auto & size_limits = step.getSettings().set_size_limits;
    const bool has_size_limits = size_limits.max_rows != 0 || size_limits.max_bytes != 0;
    return !step.preservesInputOrder() && !has_size_limits && !step.getColumnNames().empty();
}

/// See `AggregationImplementation::StrategyEnumerator::addShuffleAggregation` for why each condition disables it.
bool isAggregationShuffleApplicable(const AggregatingStep & step)
{
    return !step.getParams().keys.empty() && !step.isGroupingSets() && !step.getParams().overflow_row
        && step.getParams().max_rows_to_group_by == 0;
}

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
