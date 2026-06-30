#pragma once

#include <Core/ColumnNumbers.h>
#include <Processors/Transforms/ChunkRouting.h>
#include <Processors/Transforms/HotKeyState.h>

namespace DB
{

/// Routing selectors for sharded aggregation, built on a shared `HotKeyState`.
///
/// A selector looks at one input chunk and returns a `ChunkRouting`. We build two of them. The input
/// selector sends every row whose key is hot (frequent) to the merge path and every other row to a cold
/// shard chosen from its key hash; it decides what is hot with the `HotKeyState` membership test. (The
/// merge path is so named because its keys, unlike the hash-disjoint cold shards, can appear in several
/// streams and so need a merge; the hot keys are aggregated per stream and their states merged into the
/// owning cold shard.) Some rows of a hot key slip into a cold shard before we detect that the key is hot
/// (its "residue"). The cold-hash selector runs later, on the aggregated states, and routes each state to
/// the cold shard that owns its key — the same shard that holds the residue — using the same hash as the
/// input selector's cold path.

/// Builds the selector for the input split (one input, `num_cold_shards` + 1 outputs). We send a cold row
/// to its hash shard, numbered 0 to `num_cold_shards` - 1, and a row on the merge path to `merge_output`
/// (the port leading to the merge path; the caller owns the port layout and passes it in). The selector
/// also carries the warmup detector, which counts keys on the fly and adds the hot ones to the shared
/// `state`.
///
/// The warmup also drives the low-cardinality fallback: if it finds the key cardinality too low to be worth
/// scattering, the selector routes the whole chunk to `merge_output` (`ChunkRouting::whole_chunk_output`, so
/// no per-row hash or selector is paid), turning sharding into a per-stream aggregate plus a cheap merge at
/// the cold shards — i.e. default aggregation behavior — without the scatter copy.
ChunkRoutingSelector
makeInputHotColdSelector(HotKeyStatePtr state, ColumnNumbers key_positions, size_t num_cold_shards, size_t merge_output);

/// Builds the selector that routes hot-key intermediate states to cold shards (one input, `num_cold`
/// outputs). Each row goes to the cold shard `mapToRange(hash(key))`, using the SAME hash as the cold path
/// of `makeInputHotColdSelector`, so a hot key's state lands in exactly the cold shard that holds its
/// residue. There is no hot/cold split here: every row is a hot-key state. The keys may sit at different
/// positions than in the input, so `key_positions` gives their positions in the intermediate-aggregate
/// header (GROUP BY key columns plus one intermediate aggregate-state column per aggregate function).
ChunkRoutingSelector makeColdHashSelector(ColumnNumbers key_positions, size_t num_cold_shards);

}
