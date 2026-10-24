#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <base/types.h>
#include <memory>

namespace DB
{

/// Base for all implementation strategies. Logical expressions and
/// DefaultImplementation passthrough have strategy = nullptr.
struct IImplementationStrategy
{
    virtual ~IImplementationStrategy() = default;
    [[nodiscard]] virtual String getName() const = 0;
};

/// Per-operator-family bases. Each physical strategy owns its local cost function
/// (defined in Cost.cpp, so all cost formulas stay in one file).
struct IJoinStrategy : IImplementationStrategy
{
    [[nodiscard]] virtual Cost estimateOperatorCost(const CostInputs & inputs) const = 0;
};
struct IAggregationStrategy : IImplementationStrategy
{
    [[nodiscard]] virtual Cost estimateOperatorCost(const CostInputs & inputs) const = 0;
};
struct IReadStrategy : IImplementationStrategy
{
    [[nodiscard]] virtual Cost estimateOperatorCost(const CostInputs & inputs) const = 0;
};

/// --- Join strategies ---

struct LocalJoinStrategy final : IJoinStrategy
{
    [[nodiscard]] String getName() const override { return "Local HashJoin"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct BroadcastJoinStrategy final : IJoinStrategy
{
    [[nodiscard]] String getName() const override { return "Broadcast HashJoin"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct ShuffleJoinStrategy final : IJoinStrategy
{
    [[nodiscard]] String getName() const override { return "Shuffle HashJoin"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

/// --- Aggregation strategies ---

struct LocalAggregationStrategy final : IAggregationStrategy
{
    [[nodiscard]] String getName() const override { return "LocalAggregation"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct ShuffleAggregationStrategy final : IAggregationStrategy
{
    [[nodiscard]] String getName() const override { return "ShuffleAggregation"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct PartialAggregationStrategy final : IAggregationStrategy
{
    [[nodiscard]] String getName() const override { return "PartialAggregation"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

/// --- Merge of partial aggregation states ---
/// No cost functions: `MergingAggregatedStep` is priced by its own formula, divided by the
/// parallelism of its distribution.

/// All partial states are gathered to one node and merged there.
struct LocalMergeStrategy final : IImplementationStrategy
{
    [[nodiscard]] String getName() const override { return "LocalMerge"; }
};

/// The partial states arrive shuffled by the group keys, and each node merges the states of its own keys.
struct ShuffleMergeStrategy final : IImplementationStrategy
{
    [[nodiscard]] String getName() const override { return "ShuffleMerge"; }
};

/// --- Replicated subplan ---

/// A step run identically on every node over replicated inputs.  Satisfies
/// {node_count=N, is_replicated=true} without a BroadcastExchange.  No cost function:
/// replicated expressions get parallelism 1.0, so the default per-step formulas already
/// charge the full work each node does.
struct ReplicatedSubplanStrategy final : IImplementationStrategy
{
    [[nodiscard]] String getName() const override { return "Replicated"; }
};

/// --- Read strategies ---

struct ParallelReadStrategy final : IReadStrategy
{
    [[nodiscard]] String getName() const override { return "ParallelRead"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

/// Replicated read on shared storage: every node reads the full table from object storage.
/// No network cost - data is accessed directly from S3/shared filesystem.
/// Used to satisfy `{node_count=N, is_replicated=true}` without a BroadcastExchange.
struct ReplicatedReadStrategy final : IReadStrategy
{
    [[nodiscard]] String getName() const override { return "ReplicatedRead"; }
    [[nodiscard]] Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

using ImplementationStrategyPtr = std::shared_ptr<const IImplementationStrategy>;

/// All strategies are stateless, so every rule shares one instance per strategy type and
/// expressions compare strategies by pointer.
template <typename Strategy>
const ImplementationStrategyPtr & strategySingleton()
{
    static const ImplementationStrategyPtr instance = std::make_shared<const Strategy>();
    return instance;
}

}
