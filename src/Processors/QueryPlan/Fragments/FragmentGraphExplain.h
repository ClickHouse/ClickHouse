#pragma once

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>

#include <cstddef>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;

/// Text output of `EXPLAIN PLAN fragments = 1`.
///
/// A graph with one `SingleNode` fragment (a plan that was not cut) prints exactly what `QueryPlan::explainPlan`
/// prints for the plan of the fragment, followed by an empty line and `Not distributed: <reason>` when the fragment
/// has a `fallback_reason`.
///
/// Otherwise, even for a cut plan with a single fragment, every fragment is printed as a header line `Query fragment <id> (<stage name>) · tasks=<N>`,
/// the `QueryPlan::explainPlan` output of its plan and an empty line, by descending id: the fragment that produces
/// the result comes first and data flows upward as in a plan tree. The `Exchanges` and `Levels` sections follow
/// the fragments; they refer to query fragment <id> as `F<id>`.
///
/// The constructor prepares what every part of the output shares: the print order and, with `pretty`, the names.
/// `graph` and `options` must outlive the object.
class FragmentGraphExplain
{
public:
    FragmentGraphExplain(const FragmentGraph & graph_, const ExplainPlanOptions & options_, size_t max_step_description_length_);

    void explain(WriteBuffer & buf) const;

private:
    const FragmentGraph & graph;
    const ExplainPlanOptions & options;
    const size_t max_step_description_length;

    /// The plan was not cut: it is printed as `EXPLAIN` without `fragments` prints it.
    const bool single_node;

    /// By descending id. The cutter numbers the stages in post-order, so a fragment comes before the fragments it reads,
    /// and the fragment that produces the result (`main`) comes first.
    std::vector<const QueryFragment *> print_order;

    /// With `pretty`, one registry for all fragments, so that a column, a runtime filter or a set has the same name in
    /// every fragment and in the `Exchanges` section. The names are built over the fragments joined at their exchanges,
    /// so they are the names plain `EXPLAIN` gives the plan before the cut.
    PrettyNamesPerPlan pretty_names;

    void explainSingleNode(WriteBuffer & buf) const;
    void explainFragments(WriteBuffer & buf) const;
    void explainExchanges(WriteBuffer & buf) const;
    void explainLevels(WriteBuffer & buf) const;

    /// With `pretty`, the keys and the sort columns are named as in the fragments.
    String formatKind(const FragmentExchange & exchange) const;

    /// Every receive step of a fragment with the plan of the fragment that sends to it.
    FragmentInputs getFragmentInputs() const;

    static std::string_view formatMode(ExchangeDescription::Kind mode);
    static const QueryPlan & getPlan(const QueryFragment & fragment);
    static std::optional<std::string_view> tryGetReceiveExchangeId(const IQueryPlanStep & step);
};

}
