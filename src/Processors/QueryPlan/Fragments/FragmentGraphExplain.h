#pragma once

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>

#include <cstddef>

namespace DB
{

class WriteBuffer;

/// The text outputs of a `FragmentGraph`. The graph is only the model of the fragments and the exchanges; these functions
/// consume it and write the `EXPLAIN` output. `explainAsFragments` is defined in `FragmentGraphExplain.cpp`, `explainAsTree`
/// in `FragmentGraphTree.cpp`. Both build a `PreparedFragmentGraph`, so they print the same `Exchanges` and `Levels`
/// sections and name the exchange keys the same way.
///
/// A graph with one `SingleNode` fragment (a plan that was not cut) is printed by both exactly as `QueryPlan::explainPlan`
/// prints the plan of the fragment, followed by an empty line and `Not distributed: <reason>` when the fragment has a
/// `fallback_reason`.

/// `EXPLAIN PLAN fragments = 1`.
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
/// Every step of every fragment is a box, connected to the boxes of its inputs as in a plan tree; the steps of a
/// fragment are surrounded by a frame titled `F<id> · <stage name>`; and every exchange is an edge from the send step of
/// the producer up to the matching receive step of the consumer, labelled with the exchange name and kind, its mode and
/// its buckets (`│` for a streaming exchange, `╎` for a persisted one). Each fragment is drawn under the fragment that
/// reads it. The drawing is followed by the `Exchanges` and `Levels` sections of `fragments = 1`. Step texts are cut at
/// 40 columns and the first label line at 48; the picture has no other width limit.
void explainAsTree(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length);

}
