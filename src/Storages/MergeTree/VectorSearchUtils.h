#pragma once
#include <Core/Types.h>
#include <memory>

namespace DB
{

/// A vehicle to transport elements of the SELECT query into the vector similarity index.
struct VectorSearchParameters
{
    /// Elements of the SELECT query
    String column;
    String distance_function;
    size_t limit;
    std::vector<Float64> reference_vector;

    /// Other metadata
    bool additional_filters_present; /// SELECT contains a WHERE or PREWHERE clause
    bool return_distances;
};

using OptionalVectorSearchParameters = std::optional<VectorSearchParameters>;

struct NearestNeighbours
{
    std::vector<UInt64> rows;
    std::optional<std::vector<float>> distances;
};

/// Distance kernel used for fused full-precision rescoring inside the range reader.
enum class VectorSearchKernel : uint8_t
{
    L2,
    Cosine,
};

/// Hint that instructs MergeTreeRangeReader to overwrite the USearch-quantized distances
/// in the `_distance` virtual column with full-precision kernel values computed directly
/// from the read vector column. Enables fusion of the rerank step into the reader for the
/// `vector_search_with_rescoring = 1` path.
struct FusedRescoreHint
{
    String vector_column;
    std::shared_ptr<const std::vector<Float32>> query_vector;
    VectorSearchKernel kernel;
};

}
