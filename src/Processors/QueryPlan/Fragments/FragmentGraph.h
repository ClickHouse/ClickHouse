#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <optional>
#include <vector>

namespace DB
{

/// A model of the stages ("query fragments") of a distributed query plan and of the exchanges between them,
/// built from the output of `makeDistributedPlan`. It is used to explain the shape of a distributed query.

/// An edge from a producer fragment to the fragment that consumes its output.
struct FragmentDependency
{
    enum class Type : uint8_t
    {
        /// The consumer processes the rows of the producer as they arrive.
        Streaming,
        /// The consumer cannot produce output until the producer has finished.
        Blocking,
    };

    /// Fragment id of the producer.
    UInt32 producer = 0;
    /// Id of the exchange the data flows through.
    UInt32 exchange = 0;
    Type type = Type::Streaming;
    /// Why the dependency is blocking, e.g. "build side of JoinLogical"; empty for a streaming dependency.
    String reason;
};

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
    /// The set this fragment fills, e.g. "k IN (SELECT k FROM small)".
    std::optional<String> set;
    /// Worker stages of a distributed set source: the id of the fragment that fills the set.
    std::optional<UInt32> set_fragment;
    /// The number of tasks of the stage.
    size_t tasks = 0;
    /// 0 for leaves (fragments without dependencies), the maximum for the root.
    UInt32 level = 0;
    /// Sorted by producer id.
    std::vector<FragmentDependency> depends_on;
    /// The plan of the stage (not owned); nullptr in hand-built graphs of tests.
    const QueryPlan * plan = nullptr;
    /// Step names of the fragment in root-to-leaf order, excluding `Expression`, `Filter` and the send and receive steps.
    std::vector<String> summary;
};

/// The data transfer between two fragments.
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
    /// The estimated number of rows the producer sends, when the planner has an estimate.
    std::optional<UInt64> estimated_rows;
};

struct FragmentGraph
{
    /// Sorted by id; the fragments of the query precede those of its sets, which get ids after the main fragment of the query.
    std::vector<QueryFragment> fragments;
    /// Sorted by id.
    std::vector<FragmentExchange> exchanges;
    /// Fragment ids by level, ascending within a level; `levels[0]` are the leaves.
    std::vector<std::vector<UInt32>> levels;

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

/// Fills `level` of every fragment and `levels`; `fragments` and their `depends_on` must be set.
/// The graph must be acyclic and have exactly one root (a fragment no other fragment depends on),
/// which must have the `Initiator` role. Exposed for tests.
void computeLevels(FragmentGraph & graph);

/// The step of the consumer fragment which reads the output of a receive step,
/// ignoring the `Expression` and `Filter` steps in between.
struct ConsumerStepInfo
{
    enum class Kind : uint8_t
    {
        Other,
        Join,
        MergingAggregated,
        Sorting,
    };

    Kind kind = Kind::Other;
    /// E.g. "JoinLogical", used in the reason text.
    String step_name;
    /// `Join` only: the receive step feeds the build side of the join.
    bool receive_feeds_build_side = false;
    /// `Sorting` only.
    SortingStep::Type sorting_type = SortingStep::Type::Full;
};

/// Pure classification of one dependency; `reason` is set for a blocking dependency and cleared otherwise.
/// A `SetValues` exchange is always blocking (the set must be complete), then a `Materialised` exchange,
/// otherwise the consumer step decides. Exposed for tests.
FragmentDependency::Type classifyDependency(
    const ConsumerStepInfo & consumer, FragmentExchange::Kind kind, FragmentExchange::Mode mode, String & reason);

/// Multi-line debug text of the model (for `LOG_TEST` and tests).
String dumpFragmentGraph(const FragmentGraph & graph);

}
