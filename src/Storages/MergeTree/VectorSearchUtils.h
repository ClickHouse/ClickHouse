#pragma once
#include <Core/Types.h>

#include <Storages/MergeTree/MarkRange.h>

#include <boost/container/small_vector.hpp>

#include <optional>
#include <utility>
#include <vector>

namespace DB
{

class MergeTreeIndexGranularity;

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

/// PK ranges for the current data range (see filterMarksUsingIndex).
struct GranuleRowFilter
{
    const MergeTreeIndexGranularity * index_granularity;
    MarkRanges pk_ranges;
    size_t index_mark;
    size_t skip_index_granularity;
    /// Precomputed [row_begin, row_end) part-level intervals from pk_ranges intersected with the skip-index granule.
    /// Must be populated by the caller before passing to granuleLocalKeyAllowed — the predicate reads only this field.
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
