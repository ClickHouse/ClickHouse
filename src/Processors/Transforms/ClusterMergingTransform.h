#pragma once

#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/finalizeChunk.h>

namespace DB
{

/// Merges adjacent groups within a specified distance for a GROUP BY WITH CLUSTER key.
/// Receives non-finalized aggregate states, sorts by all keys, merges adjacent rows
/// whose cluster key values differ by at most the specified distance,
/// then finalizes and outputs results.
class ClusterMergingTransform : public IAccumulatingTransform
{
public:
    ClusterMergingTransform(
        SharedHeader header_,
        AggregatingTransformParamsPtr params_,
        String cluster_key_name_,
        Float64 cluster_distance_);

    String getName() const override { return "ClusterMergingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    String cluster_key_name;
    Float64 cluster_distance;
    ColumnsMask aggregates_mask;

    Chunks consumed_chunks;
    bool generated = false;
};

}
