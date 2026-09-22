#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <array>
#include <unordered_map>

namespace DB::QueryPlanOptimizations
{

/// Apply a unary plan step to statistics already estimated for its input. Returns `std::nullopt`
/// when the step is not a supported unary statistics transformation.
std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats);

struct RelationStatsOptions
{
    /// Propagate the row estimate and column statistics of an already optimized `JoinStepLogical`
    /// to its parents. Off by default: join reordering, join-algorithm selection
    /// (`rhs_size_estimation`), the small-probe decline and broadcast/shuffle decisions consume
    /// `estimated_rows` without checking how it was derived, and doing so regressed performance when
    /// join-key NDVs were unknown (#97114, reverted in #99957 and backported to 26.2/26.3; robust fix
    /// pending in #101398). Consumers that gate every decision on `ColumnStatsProvenance` may opt in:
    /// a sub-join output carries `Unsupported`, so they fail closed.
    bool propagate_join_estimates = false;
};

static_assert(sizeof(RelationStatsOptions) == sizeof(bool), "RelationStatsCache mode index must include every option");

/// Statistics memoized for one optimizer pass. The two maps keep the default and
/// `propagate_join_estimates` modes separate because an optimized logical join intentionally has
/// different results in those modes. Entries whose subtrees cannot observe that option are shared
/// between both maps.
class RelationStatsCache
{
public:
    /// A plan rewrite changed the step or children at this address. This does not walk ancestors;
    /// estimates which cross a mutable JoinStepLogical or CommonSubplanReferenceStep are therefore
    /// invocation-local. Other mutating code must invalidate or rebind every previously estimated
    /// node it changes.
    void invalidate(const QueryPlan::Node & node);

    /// `makeExpressionNodeOnTopOf` moves the old node to a new address and installs a wrapper at the
    /// old one. Preserve the old subtree's estimate at its new address; the wrapper is left uncached.
    void rebindNode(const QueryPlan::Node & old_node, const QueryPlan::Node & new_node);

private:
    struct Entry
    {
        RelationStats stats;
        /// Compared by identity only and never dereferenced. The pointed-to DAG is owned by the plan,
        /// whose mutations must invalidate the affected entry before that DAG can be destroyed.
        const ActionsDAG::Node * filter = nullptr;
        bool options_independent = false;
        /// Zero means reusable for the lifetime of this pass; other values identify one estimator call.
        UInt64 invocation = 0;
    };

    using Entries = std::unordered_map<const QueryPlan::Node *, Entry>;
    std::array<Entries, 2> entries_by_mode;
    UInt64 next_invocation = 0;

    friend RelationStats estimateReadRowsCount(
        QueryPlan::Node &, const ActionsDAG::Node *, RelationStatsOptions, RelationStatsCache *);
};

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. Pass a cache to share one iterative derivation across consumers
/// in the same optimizer pass; without one, a temporary cache is used for this call.
RelationStats estimateReadRowsCount(
    QueryPlan::Node & node,
    const ActionsDAG::Node * filter = nullptr,
    RelationStatsOptions options = {},
    RelationStatsCache * cache = nullptr);

}
