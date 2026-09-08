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
    virtual String getName() const = 0;
};

/// Per-operator-family bases. Each physical strategy owns its local cost function
/// (defined in Cost.cpp, so all cost formulas stay in one file).
struct IJoinStrategy : IImplementationStrategy
{
    virtual Cost estimateOperatorCost(const CostInputs & inputs) const = 0;
};
struct IAggregationStrategy : IImplementationStrategy
{
    virtual Cost estimateOperatorCost(const CostInputs & inputs) const = 0;
};
struct IReadStrategy : IImplementationStrategy
{
    virtual Cost estimateOperatorCost(const CostInputs & inputs) const = 0;
};

/// --- Join strategies ---

struct LocalJoinStrategy final : IJoinStrategy
{
    String getName() const override { return "Local HashJoin"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct BroadcastJoinStrategy final : IJoinStrategy
{
    String getName() const override { return "Broadcast HashJoin"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct ShuffleJoinStrategy final : IJoinStrategy
{
    String getName() const override { return "Shuffle HashJoin"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

/// --- Aggregation strategies ---

struct LocalAggregationStrategy final : IAggregationStrategy
{
    String getName() const override { return "LocalAggregation"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct ShuffleAggregationStrategy final : IAggregationStrategy
{
    String getName() const override { return "ShuffleAggregation"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

struct PartialAggregationStrategy final : IAggregationStrategy
{
    String getName() const override { return "PartialAggregation"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

/// --- Replicated subplan ---

/// A step run identically on every node over replicated inputs.  Satisfies
/// {node_count=N, is_replicated=true} without a BroadcastExchange.  No cost function:
/// replicated expressions get parallelism 1.0, so the default per-step formulas already
/// charge the full work each node does.
struct ReplicatedSubplanStrategy final : IImplementationStrategy
{
    String getName() const override { return "Replicated"; }
};

/// --- Read strategies ---

struct ParallelReadStrategy final : IReadStrategy
{
    String getName() const override { return "ParallelRead"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
};

/// Replicated read on shared storage: every node reads the full table from object storage.
/// No network cost - data is accessed directly from S3/shared filesystem.
/// Used to satisfy `{node_count=N, is_replicated=true}` without a BroadcastExchange.
struct ReplicatedReadStrategy final : IReadStrategy
{
    String getName() const override { return "ReplicatedRead"; }
    Cost estimateOperatorCost(const CostInputs & inputs) const override;
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
