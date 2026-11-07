#pragma once

#include <Core/Types.h>
#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>

#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;
class IQueryPlanStep;

/// What every output of a `FragmentGraph` needs, computed once and shared by `EXPLAIN PLAN fragments = 1` and
/// `EXPLAIN PLAN tree = 1` (see `FragmentGraphExplain.h`), so that they have one name for the same objects.
///
/// `graph` and `options` must outlive the object. With `pretty`, the names point to the plans of the fragments, which
/// belong to the cut distributed plan, so the cut must outlive the object too.
struct PreparedFragmentGraph
{
    PreparedFragmentGraph(const FragmentGraph & graph_, const ExplainPlanOptions & options_);

    const FragmentGraph & graph;
    const ExplainPlanOptions & options;

    /// The graph has one `SingleNode` fragment: the plan was not cut. `fragments = 1` prints it as `EXPLAIN` without
    /// options does (`writeSingleNode`), `tree = 1` draws it without a frame.
    const bool single_node;

    /// By descending id. The cutter numbers the stages in post-order, so a fragment comes before the fragments it reads,
    /// and the fragment that produces the result (`main`) comes first. Empty for a single node.
    std::vector<const QueryFragment *> print_order;

    /// With `pretty`, one registry for all fragments, so that a column, a runtime filter or a set has the same name in
    /// every fragment and in the `Exchanges` section. The names are built over the fragments joined at their exchanges,
    /// so they are the names plain `EXPLAIN` gives the plan before the cut. For a single node, the names
    /// `QueryPlan::explainPlan` gives its plan.
    PrettyNamesPerPlan pretty_names;

    /// The names of the columns and runtime filters in `plan`, the plan of a fragment or a nested plan of a step, for
    /// `IQueryPlanStep::FormatSettings`. Without `pretty`, empty maps.
    const PrettyNames & getPrettyNames(const QueryPlan & plan) const;

    /// `hash(keys)`, `broadcast`, `gather`, `gather sorted by ...`, `any` or `set values`. With `pretty`, the keys and the
    /// sort columns are named as in the fragments.
    String formatExchangeKind(const FragmentExchange & exchange) const;

    /// `streaming` or `persisted`.
    static std::string_view formatExchangeMode(ExchangeDescription::Kind mode);

    /// Throws `LOGICAL_ERROR` when the fragment has no plan (a hand-built graph).
    static const QueryPlan & getPlan(const QueryFragment & fragment);

    /// The exchange name of a receive step, or nullptr if the step is not a receive step.
    static const String * tryGetReceiveExchangeId(const IQueryPlanStep & step);

private:
    /// Every receive step of a fragment with the plan of the fragment that sends to it.
    FragmentInputs getFragmentInputs() const;
};

/// The plan of a single node as `EXPLAIN` without options prints it, followed by an empty line and
/// `Not distributed: <reason>` when the fragment has a fallback reason.
void writeSingleNode(const PreparedFragmentGraph & prepared, WriteBuffer & buf, size_t max_step_description_length);

/// The `Exchanges:` section: one line per exchange with its kind, mode, producer and consumer, and buckets.
void writeExchangesSection(const PreparedFragmentGraph & prepared, WriteBuffer & buf);

/// The `Levels:` line: the fragment ids grouped by topological level, leaves first (see `computeLevels`).
void writeLevelsSection(const FragmentGraph & graph, WriteBuffer & buf);

}
