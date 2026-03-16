#pragma once

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

/// Per-operator-family bases. Cost functions take these as typed pointers,
/// preventing e.g. an aggregation strategy from reaching join cost logic.
struct IJoinStrategy : IImplementationStrategy {};
struct IAggregationStrategy : IImplementationStrategy {};
struct IReadStrategy : IImplementationStrategy {};

/// --- Join strategies ---

struct LocalJoinStrategy final : IJoinStrategy
{
    String getName() const override { return "LocalJoin"; }
};

struct BroadcastJoinStrategy final : IJoinStrategy
{
    String getName() const override { return "BroadcastJoin"; }
};

struct ShuffleJoinStrategy final : IJoinStrategy
{
    String getName() const override { return "ShuffleJoin"; }
};

/// --- Aggregation strategies ---

struct LocalAggregationStrategy final : IAggregationStrategy
{
    String getName() const override { return "LocalAggregation"; }
};

struct ShuffleAggregationStrategy final : IAggregationStrategy
{
    String getName() const override { return "ShuffleAggregation"; }
};

struct PartialAggregationStrategy final : IAggregationStrategy
{
    String getName() const override { return "PartialAggregation"; }
};

/// --- Read strategies ---

struct ParallelReadStrategy final : IReadStrategy
{
    String getName() const override { return "ParallelRead"; }
};

/// Replicated read on shared storage: every node reads the full table from object storage.
/// No network cost — data is accessed directly from S3/shared filesystem.
/// Used to satisfy `{node_count=N, is_replicated=true}` without a BroadcastExchange.
struct ReplicatedReadStrategy final : IReadStrategy
{
    String getName() const override { return "ReplicatedRead"; }
};

using ImplementationStrategyPtr = std::shared_ptr<const IImplementationStrategy>;

}
