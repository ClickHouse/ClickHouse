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
/// row to a cold shard chosen from its key hash; it decides what is hot with the `HotKeyState` membership
/// test. Some rows of a hot key slip into a cold shard before we detect that the key is hot (its "residue").
/// The cold-hash selector runs later, on the aggregated hot-key states, and routes each state to the cold
/// shard that owns its key — the same shard that holds the residue — using the same hash as the input
/// selector's cold path. `BufferedShardingTransform` applies a selector, but the selectors themselves do
/// not depend on it.

/// Builds the selector for the input split (one input, `num_cold_shards` + 1 outputs). We send a cold row
/// to its hash shard, numbered 0 to `num_cold_shards` - 1, and a hot row to the last port, numbered
/// `num_cold_shards`. The selector also carries the warmup detector, which counts keys on the fly and adds
/// the hot ones to the shared `state`.
std::function<IColumn::Selector(const Columns &)>
makeInputHotColdSelector(HotKeyStatePtr state, ColumnNumbers key_positions, size_t num_cold_shards);

/// Builds the selector that routes hot-key intermediate states to cold shards (one input, `num_cold`
/// outputs). Each row goes to the cold shard `mapToRange(hash(key))`, using the SAME hash as the cold path
/// of `makeInputHotColdSelector`, so a hot key's state lands in exactly the cold shard that holds its
/// residue. There is no hot/cold split here: every row is a hot-key state. The keys may sit at different
/// positions than in the input, so `key_positions` gives their positions in the intermediate-aggregate
/// header (GROUP BY key columns plus one intermediate aggregate-state column per aggregate function).
std::function<IColumn::Selector(const Columns &)> makeColdHashSelector(ColumnNumbers key_positions, size_t num_cold_shards);

}
