#pragma once
#include <Core/Types.h>
#include <Common/VectorWithMemoryTracking.h>

#include <boost/container/small_vector.hpp>

#include <optional>
#include <utility>
#include <vector>

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

/// Precomputed row filter for vector index filtered_search (see filterMarksUsingIndex).
struct GranuleRowFilter
{
    /// Precomputed granule bounds in part-level row offsets for fast predicate checks.
    size_t granule_row_base = 0;
    size_t granule_row_end = 0;
    size_t granule_row_span = 0;
    /// Precomputed [row_begin, row_end) part-level intervals from PK ranges intersected with the skip-index granule.
    /// Must be populated by the caller before passing to granuleLocalKeyAllowed.
    /// Intervals must be sorted by row_begin and non-overlapping (enforced during construction).
    boost::container::small_vector<std::pair<size_t, size_t>, 4> allowed_part_row_ranges;
};

struct ANNSearchOverrides
{
    /// Restrict graph traversal to keys/_part_offsets passing this filter.
    std::optional<GranuleRowFilter> row_filter;
    /// More in future...
};

}
