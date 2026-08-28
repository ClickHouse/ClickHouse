#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <memory>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Helpers shared by the optimization rules.

/// A Full sort with a limit is a top-N: it reduces rows, so it stays in the memo as an operator
/// (a limit-less Full sort is stripped into a sorting property). Only a Full sort takes unsorted
/// input; `FinishSorting`/`MergingSorted` need ordered input, which no rule provides.
bool isTopNSort(const IQueryPlanStep & step);

/// Node counts the rules create speculative multi-node variants at. Returns {max_node_count} -
/// the full cluster. Intermediate counts are not candidates: they multiply the search space
/// without winning on the workloads measured so far.
inline std::vector<size_t> getCandidateNodeCounts(size_t max_node_count)
{
    if (max_node_count <= 1)
        return {};
    return {max_node_count};
}

/// Stateless per-row steps that can run on any data partition independently. Implemented by
/// `DistributionPassthrough` and therefore excluded from `DefaultImplementation` - both go
/// through this one predicate so a new passthrough step type cannot end up with two
/// implementation rules or none.
bool isDistributionPassthroughStep(const IQueryPlanStep & step);

/// Builds the self-referential expression an enforcer inserts: it lives in the source
/// expression's group and its single input points back to the same group with relaxed
/// requirements, so the memo search recurses into the group to satisfy them. The enforced
/// property marks the expression for the cycle-avoidance rules. Only constructs - the caller
/// still calls `addPhysicalToMemo` with the original required properties.
GroupExpressionPtr makeEnforcerExpression(
    const GroupExpressionPtr & source,
    QueryPlanStepPtr step,
    ExpressionProperties input_required,
    ExpressionProperties output_properties,
    EnforcedProperty enforced);

/// Clones a read with its marks pinned into `target_buckets` coordinator-computed buckets and
/// wraps it into a physical expression with the given strategy and output distribution. Returns
/// nullptr when the read does not split into exactly the requested count (an unsupported
/// feature, nothing to read, an unsplittable FINAL); `actual_buckets` reports the count for the
/// caller's log line.
GroupExpressionPtr tryMakeBucketedReadVariant(
    const GroupExpressionPtr & expression,
    size_t node_count,
    size_t target_buckets,
    ImplementationStrategyPtr strategy,
    const char * description_prefix,
    bool is_replicated,
    size_t & actual_buckets);

/// Clones a plan step and returns it as its concrete type; throws if the clone has another type.
template <typename Step>
std::unique_ptr<Step> cloneStepAs(const Step & step)
{
    auto clone = step.clone();
    if (typeid_cast<Step *>(clone.get()) == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Clone of '{}' has unexpected type", step.getName());
    return std::unique_ptr<Step>(static_cast<Step *>(clone.release()));
}

}
