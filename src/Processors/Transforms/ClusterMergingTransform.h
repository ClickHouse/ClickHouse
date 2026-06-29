#pragma once

#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/finalizeChunk.h>

namespace DB
{

/// Implements `GROUP BY ... WITH CLUSTER <distance>`: consumes mergeable aggregate
/// states, merges rows whose cluster keys are within `distance` of each other
/// (numeric: absolute difference; 2D tuple: Euclidean; String: Levenshtein),
/// finalizes, and emits the result.
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

    /// Scalar numeric key: bucket → adjacency merge.
    Chunk generate1D();
    /// Inline `(x, y)` tuple: grid cells of side `d / sqrt(2)` → DSU over neighbor cells.
    Chunk generate2D();
    /// `String` / `FixedString`: byte-level Levenshtein, DSU over candidate pairs.
    Chunk generateString();

    Chunks consumed_chunks;
    bool generated = false;
};

}
