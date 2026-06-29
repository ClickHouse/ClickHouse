#pragma once

#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// A cold shard of skew-robust sharded aggregation. It aggregates the shard's raw rows, injects the
/// finished hot-key states routed to it, and finalizes. See the long comment in `AggregatingStep` for the
/// full pipeline.
///
/// Has 2 input ports and 1 output port:
///   - Input 0 (raw): the shard's raw cold rows (`input_header`), consumed with `Aggregator::executeOnBlock`.
///   - Input 1 (hot states): the hot-key intermediate states whose key hashes to this shard
///     (`intermediate_header`), consumed with `Aggregator::mergeOnBlock`.
///
/// A hot key's rows that reached this cold shard before the key was detected (its "residue") are already in
/// the hashtable from input 0; merging the hot state from input 1 combines with that residue entry, so the
/// key ends up complete in exactly one shard. Both inputs are drained fully before the hashtable is
/// converted with `Aggregator::convertToChunks` and pushed downstream.
///
/// Injecting only the few hot states, instead of re-merging every cold key, keeps the bulk cold path
/// merge-free: the cold shards finalize their disjoint keys directly, and only a handful of hot states are
/// folded in.
///
/// Sharded aggregation keeps the hashtable single-level and in memory (two-level and external aggregation
/// are disabled there), so finalization is a direct `convertToChunks` rather than the
/// `ConvertingAggregatedToChunksTransform` expansion that `AggregatingTransform` uses.
class ColdShardAggregatingTransform final : public IProcessor
{
public:
    ColdShardAggregatingTransform(
        SharedHeader raw_header,
        SharedHeader intermediate_header,
        SharedHeader output_header,
        AggregatingTransformParamsPtr params_,
        bool final_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "ColdShardAggregatingTransform"; }

    Status prepare() override;
    void work() override;

private:
    static Chunk convertAggregatedChunk(Aggregator::AggregatedChunk && agg_chunk);

    AggregatingTransformParamsPtr params;
    bool final;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    AggregatedDataVariants variants;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    bool no_more_keys = false;

    /// Consume state.
    Chunk pending_chunk;
    bool pending_is_merge = false;
    bool has_pending = false;
    bool consume_finished_early = false;

    /// Generate state.
    bool converted = false;
    Aggregator::AggregatedChunks output_chunks;
};

}
