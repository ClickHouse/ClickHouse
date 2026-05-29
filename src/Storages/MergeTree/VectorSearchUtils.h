#pragma once
#include <algorithm>
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

/// Merge per-granule top-k results into a single part-wide top-k (by distance).
inline void mergeNearestNeighbours(NearestNeighbours & accumulated, NearestNeighbours && granule, size_t limit)
{
    if (granule.rows.empty())
        return;

    if (accumulated.rows.empty())
    {
        accumulated = std::move(granule);
        return;
    }

    if (!accumulated.distances.has_value() || !granule.distances.has_value())
    {
        accumulated.rows.insert(accumulated.rows.end(), granule.rows.begin(), granule.rows.end());
        return;
    }

    struct Entry
    {
        UInt64 row = 0;
        float distance = 0;
    };

    std::vector<Entry> entries;
    entries.reserve(accumulated.rows.size() + granule.rows.size());

    for (size_t i = 0; i < accumulated.rows.size(); ++i)
        entries.push_back({accumulated.rows[i], accumulated.distances->at(i)});
    for (size_t i = 0; i < granule.rows.size(); ++i)
        entries.push_back({granule.rows[i], granule.distances->at(i)});

    const size_t keep = std::min(limit, entries.size());
    auto compare = [](const Entry & a, const Entry & b) { return a.distance < b.distance; };
    if (entries.size() <= keep)
        std::ranges::sort(entries, compare);
    else
        std::partial_sort(entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(keep), entries.end(), compare);

    entries.resize(keep);
    accumulated.rows.resize(keep);
    accumulated.distances->resize(keep);
    for (size_t i = 0; i < keep; ++i)
    {
        accumulated.rows[i] = entries[i].row;
        accumulated.distances->at(i) = entries[i].distance;
    }
}

}
