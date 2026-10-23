#pragma once

#include <cstddef>

namespace DB
{

class WriteBuffer;
struct ExplainPlanOptions;
struct FragmentGraph;

/// Text output of `EXPLAIN PLAN tree = 1`: the query fragments drawn with box-drawing characters.
///
/// A graph with one fragment and no exchanges prints what `EXPLAIN PLAN fragments = 1` prints for it: the plan,
/// followed by `Not distributed: <reason>` when the fragment has a `fallback_reason`.
///
/// Otherwise every step of every fragment is a box, connected to the boxes of its inputs as in a plan tree; the steps
/// of a fragment are surrounded by a frame titled `F<id> · <stage name>`; and every exchange is an edge from the send
/// step of the producer up to the matching receive step of the consumer, labelled with the exchange name and kind,
/// its mode and its buckets (`│` for a streaming exchange, `╎` for a persisted one). Each fragment is drawn under the
/// fragment that reads it. The drawing is followed by the `Exchanges` and `Levels` sections of
/// `fragments = 1`. Step texts are cut at 40 columns and the first label line at 48; the picture has no other width
/// limit.
void explainFragmentGraphTree(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length);

}
