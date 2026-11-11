#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/Logger.h>
#include <base/types.h>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/// Weights for combining cost components into a single scalar:
///   total = work * work_weight + network * network_weight + sequential * sequential_weight
///
/// Cost approximates wall-clock time on the bottleneck node.  Parallelism factor:
///   partitioned data (`is_replicated = false`): each node processes 1/N -- divide by N
///   replicated data  (`is_replicated = true`):  each node processes all -- no division
///
/// Three dimensions:
///   - `work`: rows or bytes processed, divided by parallelism (covers both CPU and I/O).
///     NOTE: the unit is operator-dependent - scan and materialization terms are in bytes
///     while probe/sort terms count rows - so `work_weight` compares mixed units and
///     cross-operator `work` ratios are approximate. TODO: normalize the units.
///   - `network`: bytes transferred over the network between nodes
///   - `sequential`: single-threaded phases (hash table builds, merge cursors) that
///     cannot be parallelized within a node.  Its weight relative to `work_weight`
///     approximates the number of parallel threads per node (only loosely, given the
///     mixed `work` units above).
///
/// Broadcast vs shuffle differentiation:
///   - work: broadcast builds the full hash table on every node
///     (`right_rows * hash_table_build_factor`), shuffle builds 1/N of it per node
///   - network:    both modeled by their respective Exchange children
///
/// Configurable at query time via `SET param__internal_cascades_cost_config = '<json>'`.
struct CostConfig
{
    Float64 work_weight = 1.0;            /// Parallelizable work (scans, expression eval, I/O).
    Float64 network_weight = 1.0;         /// Per-byte network transfer.
    /// Single-threaded phases (gather/scatter funnels, merge cursors). The weight is the
    /// per-node thread count: work parallelizes across the threads of a node while a serial
    /// phase holds one thread, so one serial row costs about `threads` work rows (Brent's law:
    /// wall-clock ~ work / threads + serial path).
    Float64 sequential_weight = 32.0;
    /// Fixed per-exchange latency (connection setup, metadata), in sequential rows. The
    /// weighted value (~1e5 work units) keeps a plan over a small input local: distribution
    /// cannot pay for its setup there.
    Float64 exchange_fixed_overhead = 3000.0;

    /// Per-operator constants of the model. The defaults are the model; overrides are for experiments.
    Float64 expression_cost_per_row = 0.1;   /// Expressions and filters do little work per row.
    Float64 hash_table_build_factor = 2.0;   /// A hash table insert costs about two probes.
    /// Source steps the model knows nothing about. Large enough to dominate typical plans, so
    /// an unknown leaf is avoided when a modeled alternative exists, but finite, so a plan that
    /// has to contain one can still be built.
    Float64 unknown_leaf_cost = 100500;
    Float64 funnel_sequential_cost_per_row = 1.0; /// Gather/scatter push every row through one stream endpoint.
    Float64 merge_sequential_cost_per_row = 1.0;  /// N-way merge of sorted streams advances one cursor at a time.

    String dump() const;
};

CostConfig parseCostConfig(const String & json_str);

struct Cost
{
    Float64 work = 0;       /// Rows/bytes processed, divided by parallelism.
    Float64 network = 0;    /// Bytes transferred over network.
    Float64 sequential = 0; /// Single-threaded phases (hash builds, merges).

    Float64 total(const CostConfig & config) const
    {
        /// If any component is infinite the plan is impossible; return infinity directly. Multiplying
        /// first would let a zero weight turn `inf * 0` into NaN, and a NaN cost compares as neither
        /// better nor worse, so an impossible plan could be picked as best.
        if (!std::isfinite(work) || !std::isfinite(network) || !std::isfinite(sequential))
            return std::numeric_limits<Float64>::infinity();

        return work * config.work_weight
             + network * config.network_weight
             + sequential * config.sequential_weight;
    }

    static Cost infinity()
    {
        return Cost{
            .work = std::numeric_limits<Float64>::infinity(),
            .network = std::numeric_limits<Float64>::infinity(),
            .sequential = std::numeric_limits<Float64>::infinity(),
        };
    }

    Cost & operator+=(const Cost & other)
    {
        work += other.work;
        network += other.network;
        sequential += other.sequential;
        return *this;
    }

    String dump(const CostConfig & config) const;
};

struct ExpressionCost
{
    Cost cost;          /// Cost of this expression only
    Cost subtree_cost;  /// Total cost of the whole subtree (this expression and all its children)
    /// False when some input has no implementation for its required properties: no plan can be
    /// built from this expression, so it must never be recorded as a group's best.
    bool buildable = true;
};


class Memo;
class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

using GroupId = size_t;

class IQueryPlanStep;
struct IImplementationStrategy;

/// Everything the local (single-operator) cost functions may read. Deliberately holds no memo
/// access, so operator costing stays a pure function of statistics and configuration.
struct CostInputs
{
    /// The plan step; used by the type dispatcher, ignored by the strategy cost functions.
    const IQueryPlanStep * step = nullptr;
    const ExpressionStatistics & output_stats;
    /// Per-input statistics, aligned with the expression inputs; an entry is null when the
    /// input group has no derived statistics.
    std::vector<const ExpressionStatistics *> input_stats;
    /// Partitioned = node_count per node; replicated = 1 (each node does the full work).
    Float64 parallelism = 1.0;
    /// Node count of the expression's own distribution property.
    Float64 node_count = 1.0;
    /// Physical rows through the exchange when the selected child emits more than the group
    /// statistics say (a partial top-N emits up to L rows per node). Resolved by the caller
    /// from the selected child; overrides the trimmed output row count.
    std::optional<Float64> exchange_rows_override;
    const CostConfig & config;
};

/// Local cost of one operator: dispatches to the strategy's cost function when the expression
/// has one, otherwise prices the step by its type. Pure; no memo access.
Cost estimateOperatorCost(const CostInputs & inputs, const IImplementationStrategy * strategy);

class CostEstimator
{
public:
    explicit CostEstimator(Memo & memo_)
        : memo(memo_)
    {}

    /// Local operator cost plus the subtree cost accumulated from the inputs' best
    /// implementations (infinity when an input is unsatisfiable).
    ExpressionCost estimateCost(GroupExpressionPtr expression);

private:
    Memo & memo;
    LoggerPtr log = getLogger("CostEstimator");
};

}
