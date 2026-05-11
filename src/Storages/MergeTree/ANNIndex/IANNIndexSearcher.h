#pragma once

#include <Core/Types.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

struct ANNIndexShapeFingerprint;

/// Hit returned by `IANNIndexSearcher::search`. `internal_id` is the per-group vertex id
/// assigned by the concrete algorithm and is later mapped back to the source `PartRowId`
/// by the group-level `PartRowIdMapReader`. Distinct from the table-level `ANNSearchHit`
/// in `ANNIndexManager.h`, which carries an already-resolved `PartRowId`.
struct ANNSearcherHit
{
    UInt32 internal_id;
    float distance;
};

/// Algorithm-agnostic per-index search defaults, produced by the DDL parser and consumed by
/// the searcher factory. Concrete algorithms attach their tuning knobs in subclasses
/// (e.g. `DiskANNSearchDefaults`). Owners hold a `ANNSearchDefaultsPtr`; defaults are
/// immutable once constructed so sharing by `shared_ptr` is safe and keeps the enclosing
/// structs copyable.
class IANNSearchDefaults
{
public:
    virtual ~IANNSearchDefaults() = default;
};
using ANNSearchDefaultsPtr = std::shared_ptr<IANNSearchDefaults>;

/// Per-query overrides for graph-search tuning. Zero means "use the value baked into the index
/// at DDL time". `search_list_size` and `beam_width` are common to all graph-based ANN
/// algorithms supported here (currently DiskANN), so they sit on the algorithm-neutral
/// interface; algorithm-specific knobs (e.g. PQ rerank size) belong on concrete subclasses.
struct ANNSearchOverrides
{
    size_t search_list_size = 0;
    size_t beam_width = 0;
};

/// Per-group vector searcher. Concrete implementations bundle the FFI state and the tuning
/// defaults supplied at construction time. `search_list_size` / `beam_width` may be overridden
/// per query through `ANNSearchOverrides`; other algorithm-specific knobs remain at construction.
class IANNIndexSearcher
{
public:
    virtual ~IANNIndexSearcher() = default;

    virtual std::vector<ANNSearcherHit> search(
        const float * query,
        size_t query_dim,
        size_t k,
        const ANNSearchOverrides & overrides) const = 0;

    /// Stateless batched distance kernel matching the index's metric. Computes
    ///   `out[i] = distance(query, candidates + i * dim)` for `i` in `[0, n)`.
    ///
    /// The kernel is the same one used inside the index's graph search, so distances
    /// returned here are numerically consistent with what `search` returns. Intended for
    /// the unindexed-parts code path so that benchmarks can measure the algorithmic
    /// speed-up of the index itself without confounding it with a different SIMD kernel.
    ///
    /// Numeric semantics depend on the underlying metric: e.g. DiskANN's `L2` returns
    /// squared L2 (not the un-squared form computed by SQL `L2Distance`); callers that
    /// mix this output with SQL-distance results are responsible for the discrepancy.
    ///
    /// `query_dim` must match the index dimension; `candidates` must point to
    /// `n * query_dim` floats laid out row-major; `out` to `n` floats.
    virtual void computeDistances(
        const float * query,
        size_t query_dim,
        const float * candidates,
        size_t n_candidates,
        float * out) const = 0;
};
using IANNIndexSearcherPtr = std::shared_ptr<IANNIndexSearcher>;

/// Open a searcher for an already-built group. Dispatches on `shape.algorithm`; currently
/// only `"diskann"` is supported. Throws `NOT_IMPLEMENTED` for unknown algorithms and
/// `LOGICAL_ERROR` if `defaults` has a concrete type that does not match `shape.algorithm`.
IANNIndexSearcherPtr createANNIndexSearcher(
    const ANNIndexShapeFingerprint & shape,
    const std::string & index_directory,
    const IANNSearchDefaults & defaults);

}
