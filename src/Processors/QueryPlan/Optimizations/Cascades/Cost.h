#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/Logger.h>
#include <base/types.h>
#include <limits>
#include <memory>

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
///   - `work`: rows or bytes processed, divided by parallelism (merges old cpu + io)
///   - `network`: bytes transferred over the network between nodes
///   - `sequential`: single-threaded phases (hash table builds, merge cursors) that
///     cannot be parallelized within a node.  Its weight relative to `work_weight`
///     approximates the number of parallel threads per node.
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

class JoinStepLogical;
class ReadFromMergeTree;
class FilterStep;
class ExpressionStep;
class AggregatingStep;

struct IJoinStrategy;
struct IAggregationStrategy;
struct IReadStrategy;

class CostEstimator
{
public:
    explicit CostEstimator(Memo & memo_)
        : memo(memo_)
    {}

    ExpressionCost estimateCost(GroupExpressionPtr expression);

private:
    ExpressionCost estimateHashJoinCost(
        const JoinStepLogical & join_step,
        const IJoinStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics,
        Float64 parallelism);

    ExpressionCost estimateReadCost(
        const ReadFromMergeTree & read_step,
        const IReadStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        Float64 distribution_node_count);

    ExpressionCost estimateAggregationCost(
        const AggregatingStep & aggregating_step,
        const IAggregationStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & input_statistics,
        Float64 parallelism);

    Memo & memo;
    LoggerPtr log = getLogger("CostEstimator");
};

}
