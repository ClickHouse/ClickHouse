#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>
#include <cmath>
#include <limits>

namespace DB
{

ReplicationDecision decideReplicationOf(const Group & group, const OptimizerContext & context, const String & rule_name, bool repeats_scan)
{
    /// Without statistics the size is unknown and has no bound.
    ReplicationSize size;
    if (group.statistics)
    {
        const auto & statistics = *group.statistics;
        if (!statistics.rows_unknown)
            size.estimated_rows = ceilToRowCount(statistics.estimated_row_count);
        size.estimate_from_defaults = statistics.estimate_from_defaults;
        if (statistics.max_row_count < Float64(std::numeric_limits<UInt64>::max()))
            size.max_rows = ceilToRowCount(statistics.max_row_count);
        size.bytes_per_row = statistics.estimated_bytes_per_row;
        if (repeats_scan && statistics.physical_read_bytes > 0)
            size.scan_bytes = ceilToRowCount(statistics.physical_read_bytes);
    }

    const ReplicationBudget budget{.max_rows = std::nullopt, .max_bytes = context.max_bytes_to_broadcast};
    const auto decision = decideReplication(size, budget);
    LOG_TEST(getLogger(rule_name), "{}", decision.describe(size, budget));
    return decision;
}

bool isTopNSort(const IQueryPlanStep & step)
{
    const auto * sorting_step = typeid_cast<const SortingStep *>(&step);
    return sorting_step != nullptr && sorting_step->getType() == SortingStep::Type::Full && sorting_step->getLimit() > 0;
}

bool isDistributionPassthroughStep(const IQueryPlanStep & step)
{
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
