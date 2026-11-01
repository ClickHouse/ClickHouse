#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <optional>
#include <vector>

namespace DB
{

/// A model of the stages ("query fragments") of a distributed query plan and of the exchanges between them,
/// built from the output of `makeDistributedPlan`. It is used to explain the shape of a distributed query.

/// One stage of a distributed query plan.
struct QueryFragment
{
    enum class ExecutionMode : uint8_t
    {
        /// A stage of a cut plan, run as tasks by the scheduler.
        Distributed,
        /// A plan that was not cut; runs as one pipeline in the initiator process:
        /// the query after a fallback to local execution, or the fragment that fills an `IN`-subquery set.
        SingleNode,
    };

    /// The stage id assigned by the cutter: N for `stage_N`, the next number for `main`. The cutter numbers the stages
    /// in post-order over the plan, visiting the inputs of a step from left to right: a stage gets a higher id than
    /// every stage it reads from, and every stage under an input of a step gets a lower id than every stage under the
    /// next input (e.g. the left side of a join before the right side).
    UInt32 id = 0;
    /// The name of the stage in the executor, to join with logs and traces.
    String stage_name;
    ExecutionMode execution_mode = ExecutionMode::Distributed;
    /// `SingleNode` only: why the plan was not cut.
    std::optional<String> fallback_reason;
    /// What the fragment computes when its stage name does not tell, e.g. the set it fills: "k IN (SELECT k FROM small)".
    std::optional<String> description;
    /// The number of tasks of the stage.
    size_t tasks = 0;
    /// The plan of the stage (not owned); nullptr in hand-built graphs of tests.
    const QueryPlan * plan = nullptr;
};

struct ExchangeStatistics
{
    /// The estimated number of rows the producer sends, when the planner has an estimate.
    std::optional<UInt64> estimated_rows;
};

/// The edge from a producer fragment to the fragment that consumes its output. Every dependency between two
/// fragments goes through exactly one exchange, and the exchanges are the only edges of the graph: the inputs of a
/// fragment are the exchanges whose `to` is its id. The send and receive steps stay in the plans of the fragments.
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
    /// The executor's transport. `Streaming`: the data is sent over the network while it is produced; the producer
    /// and the consumer run at the same time. `Persisted`: the data is written to temporary files, and the consumer
    /// starts only after the producer has finished.
    ExchangeDescription::Kind mode = ExchangeDescription::Kind::Streaming;
    size_t source_buckets = 0;
    size_t destination_buckets = 0;
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

/// Builds the graph from the stages of a cut distributed plan. The merge stages of runtime filters (`filter_only`) and
/// their dependencies are not part of the graph. `distributed_plan` must outlive the graph, because the fragments point
/// to its plans.
FragmentGraph buildFragmentGraph(const DistributedQueryPlan & distributed_plan);

/// The one-fragment graph of a plan that was not cut (no exchanges, or the planner fell back to a single node).
/// The fragment has the `SingleNode` execution mode and carries `fallback_reason`.
FragmentGraph buildSingleFragmentGraph(const QueryPlan & plan, std::optional<String> fallback_reason);

/// Throws `LOGICAL_ERROR` unless the fragments and the exchanges are each in strictly ascending order of ids, every
/// exchange goes from a known fragment to a known fragment, and the graph is acyclic. Only what the renderers need is
/// checked: an inconsistent cut is still shown as it is.
void validateFragmentGraph(const FragmentGraph & graph);

/// Fragment ids by topological level, ascending within a level: the first level are the leaves.
/// A fragment is one level above its deepest producer. Computed on demand for the renderers; validates the graph.
std::vector<std::vector<UInt32>> computeLevels(const FragmentGraph & graph);

}
