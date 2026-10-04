#pragma once

#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

class ActionsDAG;

namespace QueryPlanOptimizations
{

/// Measured number of distinct values a hash table was built on for this exact key subset, if some
/// earlier query on the same subtree happened to build one. Supplied by the caller so this pass does
/// not have to know how the join reorder tracks runtime hash-table statistics.
using CachedSubsetNdvLookup
    = std::function<std::optional<UInt64>(const std::vector<const ActionsDAG::Node *> & key_nodes)>;

/// Candidates per probe row a previous execution actually saw for a hash table keyed on this exact
/// subset, if one was ever built. Preferred over the uniform estimate, which assumes every distinct
/// key is equally likely and so understates a skewed key - measured at 4.9 against a real 55 on one
/// join. Only ever available for a subset some earlier query demoted down to, so a first execution
/// is still scored from the estimate.
using MeasuredFanoutLookup
    = std::function<std::optional<double>(const std::vector<const ActionsDAG::Node *> & key_nodes)>;

/// Move equality keys out of the hash table's key set and into `JoinOperator::probe_conditions`,
/// where they are checked per candidate row during the probe, when doing so shrinks the table by
/// more than the probe-time work it creates costs.
///
/// Must run after the join order and orientation are decided - the build side, its row count and
/// its column statistics are all inputs - and before the hash table cache keys are derived, since
/// those hash the equalities left in the ON expression and so must describe the table that is
/// actually built. Returns true if at least one equality was demoted.
bool demoteHighNdvKeysToProbe(
    JoinStepLogical & join_step,
    std::optional<UInt64> build_rows,
    const std::unordered_map<String, ColumnStats> & build_column_stats,
    const CachedSubsetNdvLookup & cached_subset_ndv,
    const MeasuredFanoutLookup & measured_fanout);

}
}
