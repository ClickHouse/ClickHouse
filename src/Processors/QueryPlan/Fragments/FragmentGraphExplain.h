#pragma once

#include <cstddef>

namespace DB
{

class WriteBuffer;
struct ExplainPlanOptions;
struct FragmentGraph;

/// Text output of `EXPLAIN PLAN fragments = 1`.
///
/// A graph with one fragment and no exchanges prints exactly what `QueryPlan::explainPlan` prints for the
/// plan of the fragment, followed by an empty line and `Not distributed: <reason>` when the fragment has a
/// `fallback_reason`.
///
/// Otherwise every fragment is printed as a header line `Query fragment <id> (<stage name>) · tasks=<N>`,
/// the `QueryPlan::explainPlan` output of its plan and an empty line, by descending id: the fragment that produces
/// the result comes first and data flows upward as in a plan tree. The `Exchanges` and `Levels` sections follow
/// the fragments; they refer to query fragment <id> as `F<id>`.
void explainFragmentGraph(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length);

}
