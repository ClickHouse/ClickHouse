#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <optional>
#include <vector>

namespace DB
{

/// A model of the stages ("query fragments") of a distributed query plan and of the exchanges between them,
/// built from the output of `makeDistributedPlan`. It is used to explain the shape of a distributed query.

/// One stage of a distributed query plan.
struct QueryFragment
{
    enum class Role : uint8_t
    {
        Initiator,
        Worker,
    };

    enum class ExecutionLocation : uint8_t
    {
        /// A stage of a cut plan, run as tasks by the scheduler.
        Distributed,
        /// A plan that was not cut; runs as one pipeline in the initiator process:
        /// the query after a fallback to local execution, or the fragment that fills an `IN`-subquery set.
        SingleNode,
    };

    /// The stage id assigned by the cutter: N for `stage_N`, the next number for `main`.
    /// Ids ascend from the leaves to the initiator.
    UInt32 id = 0;
    /// The name of the stage in the executor, to join with logs and traces.
    String stage_name;
    Role role = Role::Worker;
    ExecutionLocation location = ExecutionLocation::Distributed;
    /// `SingleNode` only: why the plan was not cut.
    std::optional<String> fallback_reason;
    /// What the fragment computes when its stage name does not tell, e.g. the set it fills: "k IN (SELECT k FROM small)".
    std::optional<String> description;
    /// The number of tasks of the stage.
    size_t tasks = 0;
    /// Ids of the exchanges this fragment reads, ascending.
    std::vector<UInt32> children;
    /// The plan of the stage (not owned); nullptr in hand-built graphs of tests.
    const QueryPlan * plan = nullptr;
    /// Step names of the fragment in root-to-leaf order, excluding `Expression`, `Filter` and the send and receive steps.
    std::vector<String> summary;
};

struct ExchangeStatistics
{
    /// The estimated number of rows the producer sends, when the planner has an estimate.
    std::optional<UInt64> estimated_rows;
};

/// The edge from a producer fragment to the fragment that consumes its output. Every dependency between two
/// fragments goes through exactly one exchange; the send and receive steps stay in the plans of the fragments.
struct FragmentExchange
{
    enum class Kind : uint8_t
    {
        /// Rows are distributed by the hash of the keys.
        Hash,
        /// Every destination receives all the rows.
        Broadcast,
        /// All the rows are collected into one destination.
        Gather,
        /// Rows are distributed without regard to their values.
        Any,
        /// A built `IN`-subquery set, shipped by value inside the tasks of every fragment that reads it.
        SetValues,
    };

    enum class Mode : uint8_t
    {
        /// The data is sent over the network while it is produced.
        Pipelined,
        /// The data is written to temporary files first.
        Materialised,
    };

    enum class Type : uint8_t
    {
        /// The consumer processes the rows of the producer as they arrive.
        Streaming,
        /// The consumer cannot produce output until the producer has finished.
        Blocking,
    };

    /// The exchange id assigned by the cutter, N for `exchange_N`.
    UInt32 id = 0;
    /// The executor's exchange name, e.g. `exchange_1`, for log and trace joins.
    String name;
    /// Producer fragment id.
    UInt32 from = 0;
    /// Consumer fragment id.
    UInt32 to = 0;
    Kind kind = Kind::Any;
    /// The hash keys of a `Hash` exchange.
    Names keys;
    /// The sort order a merging `Gather` exchange maintains.
    std::optional<SortDescription> merge_sort;
    Mode mode = Mode::Pipelined;
    size_t source_buckets = 0;
    size_t destination_buckets = 0;
    Type type = Type::Streaming;
    /// Why the exchange is blocking, e.g. "build side of JoinLogical"; empty for a streaming exchange.
    String reason;
    ExchangeStatistics statistics;
};

struct FragmentGraph
{
    /// Sorted by id; the fragments of the query precede those of its sets, which get ids after the main fragment of the query.
    std::vector<QueryFragment> fragments;
    /// Sorted by id.
    std::vector<FragmentExchange> exchanges;

    /// Throws `LOGICAL_ERROR` when absent.
    const QueryFragment & getFragment(UInt32 id) const;
    /// Throws `LOGICAL_ERROR` when absent.
    const FragmentExchange & getExchange(UInt32 id) const;
};

/// Builds the graph from the stages of a cut distributed plan.
/// `distributed_plan` must outlive the graph, because the fragments point to its plans.
FragmentGraph buildFragmentGraph(const DistributedQueryPlan & distributed_plan);

/// The one-fragment graph of a plan that was not cut (no exchanges, or the planner fell back to a single node).
/// The fragment is an initiator with the `SingleNode` location and carries `fallback_reason`.
FragmentGraph buildSingleFragmentGraph(const QueryPlan & plan, std::optional<String> fallback_reason);

/// Throws `LOGICAL_ERROR` unless every child of a fragment is an exchange into it from a known fragment, every
/// exchange is the child of one fragment, the graph is acyclic, and it has exactly one root (a fragment whose
/// output no exchange reads), which has the `Initiator` role.
void validateFragmentGraph(const FragmentGraph & graph);

/// Fragment ids by topological level, ascending within a level: the first level are the leaves, the last the root.
/// A fragment is one level above its deepest producer. Computed on demand for the renderers; validates the graph.
std::vector<std::vector<UInt32>> computeLevels(const FragmentGraph & graph);

/// Pure classification of one exchange; `reason` is set for a blocking exchange and cleared otherwise.
/// A `SetValues` exchange is always blocking (the set must be complete), then a `Materialised` exchange,
/// otherwise `consumption`: how the consumer fragment uses the rows of the exchange, i.e. the first step on the way
/// from the receive step to the output of the fragment that needs the whole input it is reached through.
/// Exposed for tests.
FragmentExchange::Type classifyDependency(
    const InputConsumption & consumption, FragmentExchange::Kind kind, FragmentExchange::Mode mode, String & reason);

/// Multi-line debug text of the model (for `LOG_TEST` and tests).
String dumpFragmentGraph(const FragmentGraph & graph);

}
