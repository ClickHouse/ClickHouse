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
        Names cluster_key_names_,
        Float64 cluster_distance_,
        size_t dimensions_);

    String getName() const override { return "ClusterMergingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    Names cluster_key_names;
    Float64 cluster_distance;
    size_t dimensions;
    ColumnsMask aggregates_mask;

    /// 1D path: sort-based + bucket-optimized merging by scalar numeric key.
    Chunk generate1D();
    /// 2D path: uniform grid with cell side d/sqrt(2), adjacency graph over
    /// cells + union-find to discover connected components.
    Chunk generate2D();
    /// String path: byte-level Levenshtein distance, naive O(N^2) pairwise
    /// comparison + union-find. Distance d is interpreted as integer max edits.
    Chunk generateString();

    Chunks consumed_chunks;
    bool generated = false;
};

}
