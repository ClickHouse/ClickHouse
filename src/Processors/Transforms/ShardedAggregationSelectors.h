#pragma once

#include <functional>
#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Transforms/HotKeyState.h>

namespace DB
{

/// Per-row routing selectors for sharded aggregation, built on a shared `HotKeyState`.
///
/// A selector looks at one input chunk and picks, for each row, which output port it goes to. We build two
/// of them. The input selector sends every row whose key is hot to a dedicated hot shard and every other
/// row to a cold shard chosen from its key hash. Some rows of a hot key slip into a cold shard before we
/// detect that the key is hot (its "residue"); the divert selector runs later, after the cold shards are
/// aggregated, and separates that residue out so we can merge it together with the hot keys. Both selectors
/// decide what is hot with the same `HotKeyState` membership test. `BufferedScatterTransform` applies a
/// selector, but the selectors themselves do not depend on it.

/// Builds the selector for the input split (one input, `num_cold_shards` + 1 outputs). We send a cold row
/// to its hash shard, numbered 0 to `num_cold_shards` - 1, and a hot row to the last port, numbered
/// `num_cold_shards`. The selector also carries the warmup detector, which counts keys on the fly and adds
/// the hot ones to the shared `state`.
std::function<IColumn::Selector(const Columns &)>
makeInputHotColdSelector(HotKeyStatePtr state, ColumnNumbers key_positions, size_t num_cold_shards);

/// Builds the selector for the cold-shard divert (one input, two outputs). We send rows whose key is hot
/// (the residue) to port 0 and the genuinely cold rows to port 1. By now the rows have already been
/// aggregated but not finalized, so their header (the "intermediate-aggregate header") holds the GROUP BY
/// key columns plus one intermediate aggregate-state column per aggregate function, in place of the input's
/// argument columns. The keys may sit at different positions than in the input, so `key_positions` gives their
/// positions in that intermediate-aggregate header.
std::function<IColumn::Selector(const Columns &)> makeDivertSelector(HotKeyStatePtr state, ColumnNumbers key_positions);

}
