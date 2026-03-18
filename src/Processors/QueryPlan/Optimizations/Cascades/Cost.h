#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/Logger.h>
#include <base/types.h>
#include <limits>
#include <memory>

namespace DB
{

/// Weights for combining cost components into a single scalar:
///   total = cpu * cpu_weight + memory * memory_weight + network * network_weight
///         + io * io_weight + sequential * sequential_weight
///
/// The `sequential` component represents single-threaded work (hash table builds in
/// joins, merge phases) that cannot be parallelized within a node.  Its weight relative
/// to `cpu_weight` should approximate the number of parallel threads per node, since
/// sequential work is T× slower in wall-clock than parallel work on T threads.
/// In a distributed setting it also controls broadcast-vs-shuffle: broadcast joins have
/// `sequential = right_rows × 2` (full hash table on every node), while shuffle joins
/// have `sequential = right_rows × 2 / N` (1/N per node).  For shuffle to win over
/// broadcast on the sequential term alone: `sequential_weight > bytes_per_row × (N-1) / 2`.
/// With typical TPC-H rows (~100 bytes) on a 20-node cluster, the threshold is ~950.
///
/// Configurable at query time via `SET param__internal_cascades_cost_config = '<json>'`.
struct CostConfig
{
    Float64 cpu_weight = 1.0;             /// Parallelizable CPU work (scans, expression eval). Reference = 1.0.
    Float64 memory_weight = 1.0;          /// Memory consumption (hash tables, buffers). ~0.1-1.0 depending on pressure.
    Float64 network_weight = 1.0;         /// Per-byte network transfer. ~1.0 if bandwidth ≈ disk; higher for slow networks.
    Float64 io_weight = 1.0;              /// Per-byte I/O (S3/disk reads). ~1.0 for S3; lower for fast NVMe.
    Float64 sequential_weight = 1000.0;   /// Single-threaded phases (hash build, merge). ~threads_per_node for single-node
                                          /// decisions; ~bytes_per_row × N / 2 for broadcast-vs-shuffle threshold.
    Float64 exchange_fixed_overhead = 100.0; /// Fixed per-exchange latency (connection setup, metadata).

    String dump() const;
};

CostConfig parseCostConfig(const String & json_str);

struct Cost
{
    Float64 cpu = 0;
    Float64 memory = 0;
    Float64 network = 0;
    Float64 io = 0;
    Float64 sequential = 0;

    Float64 total(const CostConfig & config) const
    {
        return cpu * config.cpu_weight + memory * config.memory_weight
             + network * config.network_weight + io * config.io_weight
             + sequential * config.sequential_weight;
    }

    static Cost infinity()
    {
        return Cost{
            .cpu = std::numeric_limits<Float64>::infinity(),
            .memory = std::numeric_limits<Float64>::infinity(),
            .network = std::numeric_limits<Float64>::infinity(),
            .io = std::numeric_limits<Float64>::infinity(),
            .sequential = std::numeric_limits<Float64>::infinity(),
        };
    }

    Cost & operator+=(const Cost & other)
    {
        cpu += other.cpu;
        memory += other.memory;
        network += other.network;
        io += other.io;
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
        Float64 distribution_node_count);

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
        Float64 distribution_node_count);

    Memo & memo;
    LoggerPtr log = getLogger("CostEstimator");
};

}
