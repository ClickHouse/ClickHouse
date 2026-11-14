#pragma once

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>

#include <cstddef>
#include <memory>

namespace DB
{

class WriteBuffer;

namespace JSONBuilder
{
class JSONMap;
}

/// The outputs of a `FragmentGraph`. The graph is only the model of the fragments and the exchanges; these functions
/// consume it and write the `EXPLAIN` output. `explainAsFragments` is defined in `FragmentGraphExplain.cpp`, `explainAsTree`
/// in `FragmentGraphTree.cpp` and `explainAsFragmentsJSON` in `FragmentGraphJSON.cpp`. The two text outputs build a
/// `PreparedFragmentGraph`, so they print the same `Exchanges` and `Levels` sections and name the exchange keys the same
/// way. The JSON output uses the names of the plan, as `json = 1` does.
///
/// A graph with one `SingleNode` fragment (a plan that was not cut) is followed in both text outputs by an empty line and
/// `Not distributed: <reason>` when the fragment has a `fallback_reason`, and has no `Exchanges` and `Levels` sections.

/// `EXPLAIN PLAN fragments = 1`.
///
/// A single node is printed exactly as `QueryPlan::explainPlan` prints its plan.
///
/// Every fragment, even of a cut plan with a single fragment, is printed as a header line
/// `Query fragment <id> (<stage name>) · tasks=<N>`, the `QueryPlan::explainPlan` output of its plan and an empty line,
/// by descending id: the fragment that produces the result comes first and data flows upward as in a plan tree. The
/// `Exchanges` and `Levels` sections follow the fragments; they refer to query fragment <id> as `F<id>`. With `pretty`,
/// a column, a runtime filter or a set has the same name in every fragment and in the `Exchanges` section.
void explainAsFragments(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length);

/// `EXPLAIN PLAN tree = 1`: the query fragments drawn with box-drawing characters.
///
/// Every step of every fragment except an `Expression` step is a box, connected to the boxes of its inputs as in a plan
/// tree; the box of the input of an `Expression` step takes its place. A box shows the step name and description, and
/// under a separator the lines `explainStepDetails` writes for the step with `options`. The steps of a
/// fragment are surrounded by a frame titled `F<id> · <stage name>`; and every exchange is an edge from the send step of
/// the producer up to the matching receive step of the consumer, labelled with the exchange name and kind, its mode and
/// its buckets (`│` for a streaming exchange, `╎` and a `⛁` before the mode for a persisted one). Each fragment is drawn
/// under the fragment that reads it. The drawing is followed by the `Exchanges` and `Levels` sections of `fragments = 1`.
/// The text of a step is wrapped to lines of 40 columns, detail lines are cut at 60 and the first label line at 48; the
/// picture has no other width limit.
///
/// A single node is drawn too, unlike with `fragments = 1`: the boxes of its steps without a frame around them.
///
/// The nested plans of a step (`IQueryPlanStep::getChildPlans`) are numbered `N1`, `N2`, ..., the box of the step ends
/// with `← N1, N2`, and every nested plan is drawn after the main drawing, without a frame, under `N<id> · nested plan
/// of <step>`, without `Expression` steps either.
void explainAsTree(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length);

/// `EXPLAIN PLAN fragments = 1, json = 1`: the element of the one-element array of the `json = 1` document; the caller
/// wraps and formats it, with the keys in camelCase (`JSONBuilder::FormatSettings::camel_case_keys`). Defined in
/// `FragmentGraphJSON.cpp`. Names are not prettified, as in `json = 1`.
///
/// A single node yields `{"Plan": ...}`, as `json = 1` does, with `"Fallback Reason"` when the fragment has one. A cut
/// plan yields `Result Fragment`, the id of the fragment that produces the result, then `Fragments` and `Exchanges` by
/// ascending id, and `Levels` (see `computeLevels`). The plan of a fragment is flat: `Root` and `Steps`, from
/// `QueryPlan::explainPlanSteps`. The send and receive steps of an exchange have `Exchange Id`.
std::unique_ptr<JSONBuilder::JSONMap> explainAsFragmentsJSON(const FragmentGraph & graph, const ExplainPlanOptions & options);

}
