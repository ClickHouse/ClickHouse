#pragma once
#include <Core/Types.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// A vehicle to transport elements of the SELECT query into the vector similarity index.
struct VectorSearchParameters
{
    /// Elements of the SELECT query
    String column;
    String distance_function;
    size_t limit;
    VectorWithMemoryTracking<Float64> reference_vector;

    /// Other metadata
    /// True if rows may be removed after the index read (a WHERE/PREWHERE filter or a DISTINCT), so the index
    /// should over-fetch neighbours (by 'vector_search_index_fetch_multiplier') to still satisfy the LIMIT.
    bool post_read_row_reduction;
    bool return_distances;
};

using OptionalVectorSearchParameters = std::optional<VectorSearchParameters>;

struct NearestNeighbours
{
    std::vector<UInt64> rows;
    std::optional<std::vector<float>> distances;
};

}
