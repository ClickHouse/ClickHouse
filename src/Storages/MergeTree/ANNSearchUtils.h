#pragma once

#include <Core/Types.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace DB
{

class IANNIndexSearcher;
using IANNIndexSearcherPtr = std::shared_ptr<IANNIndexSearcher>;

/// Query-time parameters for ANN (Approximate Nearest Neighbor) search on table-level indexes.
///
/// Contract:
///   - `column`:              name of the vector column, e.g. "emb"
///   - `distance_function`:   one of "L2Distance" / "cosineDistance" / "dotProduct"
///   - `limit`:               user LIMIT N (after Analyzer)
///   - `reference_vector`:    reference vector as Float64 (Analyzer constant form);
///                            consumers convert to Float32 at kernel invocation sites
///   - `rescoring_factor`:    k * factor as the recall target for index search
///   - `additional_filters_present`: WHERE / PREWHERE exists on top of ReadFromMergeTree
///   - `metric_kernel`:       a borrowed `IANNIndexSearcher` from one of the active groups,
///                            used by the unindexed-parts code path when the user opts into
///                            the index's distance kernel (`vector_search_unindexed_metric_source = 'index'`).
///                            `nullptr` when no group is currently active — in that case the
///                            unindexed path silently falls back to the SQL distance function.
struct ANNSearchParameters
{
    String column;
    String distance_function;
    size_t limit;
    std::vector<Float64> reference_vector;
    size_t rescoring_factor = 1;
    bool additional_filters_present = false;
    IANNIndexSearcherPtr metric_kernel;
    /// When set, skip the table-level ANN index lookup so every part goes through the
    /// unindexed-parts dispatch. Combined with `vector_search_unindexed_metric_source` this
    /// gives an apples-to-apples brute-force baseline for benchmarks: same kernel as the
    /// index path, no graph search.
    bool force_brute_force = false;
    /// Per-query overrides for graph-search tuning. Zero means "use the value baked into the
    /// index at DDL time". Algorithm-specific knobs live on the concrete searcher; these two
    /// are universal for graph-based ANN (DiskANN, HNSW, …) so they sit at the routing layer.
    size_t search_list_size = 0;
    size_t beam_width = 0;
};

/// Per-part routing result for ANN index search.
///
/// Contract:
///   - `block_coords.size() == distances.size()`
///   - `block_coords[i]` corresponds to `distances[i]`
///   - `distances` stored as Float32 (matches Array(Float32) vec col natural type)
///
/// Tri-state semantics for `RangesInDataPartReadHints::ann_search_results`:
///   - `nullopt`                              — part is unindexed (not covered by ANN index)
///   - `has_value() && block_coords.empty()`  — part is covered but no hits for this query
///   - `has_value() && !block_coords.empty()` — part is indexed with hits
struct ANNSearchResults
{
    std::vector<std::pair<UInt64, UInt64>> block_coords;  /// (block_number, block_offset)
    std::vector<Float32> distances;
};

}
