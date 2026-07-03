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
///   - `work`: rows or bytes processed, divided by parallelism (merges old cpu + io).
///     NOTE: the unit is currently operator-dependent — scans and materialization terms are
///     byte-denominated while probe/sort terms count rows — so `work_weight` compares
///     mixed units. Unit normalization is planned; until then treat cross-operator
///     `work` ratios as approximate.
///   - `network`: bytes transferred over the network between nodes
///   - `sequential`: single-threaded phases (hash table builds, merge cursors) that
///     cannot be parallelized within a node.  Its weight relative to `work_weight`
///     approximates the number of parallel threads per node (only loosely, given the
///     mixed `work` units above).
///
/// Broadcast vs shuffle differentiation:
///   - sequential: broadcast = `right_rows * 2` (full HT), shuffle = `right_rows * 2 / N`
///   - network:    both modeled by their respective Exchange children
///
/// Configurable at query time via `SET param__internal_cascades_cost_config = '<json>'`.
struct CostConfig
{
    Float64 work_weight = 1.0;            /// Parallelizable work (scans, expression eval, I/O).
    Float64 network_weight = 1.0;         /// Per-byte network transfer.
    Float64 sequential_weight = 1000.0;   /// Single-threaded phases (hash build, merge).
    Float64 exchange_fixed_overhead = 100.0; /// Fixed per-exchange latency (connection setup, metadata).

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
};

struct ExpressionCost
{
    Cost cost;          /// Cost of this expression only
    Cost subtree_cost;  /// Total cost of the whole subtree (this expression and all its children)
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
    /// Physical rows through a gather over a partial top-N: min(input_rows, L * producers).
    /// Computed by the caller from the memo; overrides the trimmed output row count.
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
