#pragma once
#include <algorithm>
#include <optional>
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
    bool additional_filters_present; /// SELECT contains a WHERE or PREWHERE clause
    bool return_distances;
};

using OptionalVectorSearchParameters = std::optional<VectorSearchParameters>;

struct NearestNeighbours
{
    std::vector<UInt64> rows;
    std::optional<std::vector<float>> distances;
};

/// Row ids from vector index search are local to a skip-index granule; add the granule's first row in the part.
inline void convertNearestNeighboursToPartOffsets(NearestNeighbours & neighbours, UInt64 granule_begin_row)
{
    for (auto & row : neighbours.rows)
        row += granule_begin_row;
}

/// Merge per-granule nearest-neighbour results into a single part-wide set.
inline void mergeNearestNeighbours(NearestNeighbours & accumulated, NearestNeighbours && granule)
{
    if (granule.rows.empty())
        return;

    accumulated.rows.insert(accumulated.rows.end(), granule.rows.begin(), granule.rows.end());

    if (accumulated.distances.has_value() && granule.distances.has_value())
        accumulated.distances->insert(accumulated.distances->end(), granule.distances->begin(), granule.distances->end());
    else
        accumulated.distances.reset();
}

/// Keep only the best `limit` neighbours (smallest distance). If distances are unavailable, keep first `limit`.
inline void truncateNearestNeighbours(NearestNeighbours & neighbours, size_t limit)
{
    if (neighbours.rows.size() <= limit)
        return;

    if (!neighbours.distances.has_value())
    {
        neighbours.rows.resize(limit);
        return;
    }

    struct Entry
    {
        UInt64 row = 0;
        float distance = 0;
    };

    std::vector<Entry> entries;
    entries.reserve(neighbours.rows.size());
    for (size_t i = 0; i < neighbours.rows.size(); ++i)
        entries.push_back({neighbours.rows[i], neighbours.distances->at(i)});

    auto compare = [](const Entry & a, const Entry & b) { return a.distance < b.distance; };
    std::partial_sort(entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(limit), entries.end(), compare);
    entries.resize(limit);

    neighbours.rows.resize(limit);
    neighbours.distances->resize(limit);
    for (size_t i = 0; i < limit; ++i)
    {
        neighbours.rows[i] = entries[i].row;
        neighbours.distances->at(i) = entries[i].distance;
    }
}

}
