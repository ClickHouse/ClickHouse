#pragma once

#include <Interpreters/Aggregator.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Aggregation transform for sharded aggregation.
/// Each shard has its own independent hash table — no merging needed because in
/// this step, identical GROUP BY keys always land on the same shard.
///
/// Input: chunks with materialized payload columns + ShardedChunkInfo (from ScatterByHashTransform) that
/// contains the row indices for this shard and the precomputed hash values for sharding.
/// Output: aggregated blocks (from hash table conversion after all input consumed).
///
/// Builds its own aggregate instructions from the payload columns on the first chunk,
/// then updates only the column pointers on subsequent chunks. All chunks must have
/// the same payload layout (column count and order), which is guaranteed by the preceding ScatterByHashTransform.
class ShardedAggregatingTransform : public IProcessor
{
public:
    ShardedAggregatingTransform(SharedHeader header, AggregatingTransformParamsPtr params_);

    String getName() const override { return "ShardedAggregatingTransform"; }

    Status prepare() override;
    void work() override;

private:
    void consume(Chunk chunk);
    void initGenerate();

    AggregatingTransformParamsPtr params;
    AggregatedDataVariants variants;

    /// State tracking.
    bool is_consume_finished = false;
    bool is_generate_initialized = false;
    bool has_input = false;
    Chunk current_chunk;

    /// Aggregate instruction state — built once, column pointers updated per chunk.
    Aggregator::AggregateColumns aggregate_columns_holder;
    Aggregator::AggregateFunctionInstructions aggregate_instructions;

    /// Output chunks from hash table conversion.
    Chunks output_chunks;
    size_t output_chunk_idx = 0;
};

}
