#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StorageLayout.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Core/Joins.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Memo;
class ReadFromMergeTree;
struct DistributionDescription;

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

/// Steps that can run on any data partition independently: stateless per-row steps and a preliminary
/// `DISTINCT`, which deduplicates each stream on its own. Implemented by
/// `DistributionPassthrough` and therefore excluded from `DefaultImplementation` - both go
/// through this one predicate so a new passthrough step type cannot end up with two
/// implementation rules or none.
bool isDistributionPassthroughStep(const IQueryPlanStep & step);

/// Builds the self-referential expression an enforcer inserts: it lives in the source
/// expression's group and its single input points back to the same group with relaxed
/// requirements, so the memo search recurses into the group to satisfy them. The expression
/// records `required_properties`, the ones that the enforcer rule runs for. Only constructs -
/// the caller still calls `addPhysicalToMemo` with the same required properties.
GroupExpressionPtr makeEnforcerExpression(
    const GroupExpressionPtr & source,
    QueryPlanStepPtr step,
    ExpressionProperties input_required,
    ExpressionProperties output_properties,
    const ExpressionProperties & required_properties);

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

/// A side is preserved when the join never emits rows with that side null/default-extended:
/// its key columns are real values in every output row. Only a preserved side's key columns can
/// be advertised as the output distribution of a join.
bool joinPreservesLeftSide(JoinKind kind);
bool joinPreservesRightSide(JoinKind kind);

/// The layouts that the result of the group can have with no exchange (see `StorageLayout`), in the column
/// names of the group's output. A rule that needs rows with equal keys on one node can require one of them
/// from this input in place of a hash distribution. Found from the table reads below the group on first
/// use; empty unless `distributed_plan_use_partition_key_layout` is set.
const StorageLayouts & getDeliveredStorageLayouts(Memo & memo, GroupId group_id);

/// Finds the partition key component of the read's table that is `distribution.layout_function` over
/// `distribution.columns`. Returns its number, or nullopt when the table has no such component.
std::optional<size_t> findPartitionKeyComponent(const ReadFromMergeTree & read_step, const DistributionDescription & distribution);

/// Clones a read with its parts put into `distribution.node_count` buckets by the value of the partition key
/// component `component_index`, and wraps it into a physical expression that delivers `distribution`.
/// Returns nullptr when the read cannot be split this way (`FINAL`, nothing to read, an unsupported feature).
GroupExpressionPtr tryMakePartitionedReadVariant(
    const GroupExpressionPtr & expression, size_t component_index, const DistributionDescription & distribution);

/// Pairs (column of the left input, column of the right input) that a join compares for equality.
using JoinEqualities = VectorWithMemoryTracking<std::pair<String, String>>;

/// The key columns by whose ranges the table reads below the group can divide its result (see
/// `KeyRangeLayoutCandidate`), in the column names of the group's output. Found on first use; empty unless
/// `distributed_plan_use_primary_key_range_layout` is set.
const KeyRangeLayoutCandidates & getKeyRangeLayoutCandidates(Memo & memo, GroupId group_id);

/// The number of the first key columns of the two candidates that have equal types and that `equalities`
/// connect, column by column. 0 when the candidates cannot be divided at the same borders.
size_t getCommonKeyRangePrefix(
    const KeyRangeLayoutCandidate & left, const KeyRangeLayoutCandidate & right, const JoinEqualities & equalities);

/// The function that divides the rows of the table reads `read_groups` into `node_count` ranges of their
/// first key columns with `key_types`. The borders come from the primary index of the parts that all the
/// reads selected, so that the ranges hold about the same number of rows. The memo keeps the function: each
/// rule that asks for the same reads gets the same one. Returns nullptr when the reads select no parts.
StorageLayoutFunctionPtr getKeyRangeLayoutFunction(
    Memo & memo, const VectorWithMemoryTracking<GroupId> & read_groups, const DataTypes & key_types, bool in_reverse_order, size_t node_count);

/// Clones a read with its marks put into `distribution.node_count` buckets at the borders of the key range
/// function of `distribution`, and wraps it into a physical expression that delivers `distribution`.
/// Returns nullptr when the required columns are not the first key columns of the table, or when the read
/// cannot be split this way (`FINAL`, nothing to read, an unsupported feature).
GroupExpressionPtr tryMakeKeyRangeReadVariant(const GroupExpressionPtr & expression, const DistributionDescription & distribution);

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
