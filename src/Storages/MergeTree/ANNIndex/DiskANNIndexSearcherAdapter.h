#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/IANNIndexSearcher.h>
#include <Storages/MergeTree/DiskANNIndex.h>

namespace DB
{

/// DiskANN tuning knobs wrapped as an `IANNSearchDefaults` so they can flow through the
/// algorithm-neutral layers (`ANNIndexManager::Config`, `ANNIndexDefinition`, `ANNIndexGroup`)
/// without exposing `DiskANNSearchOptions` outside of `DiskANN*` translation units.
struct DiskANNSearchDefaults : public IANNSearchDefaults
{
    DiskANNSearchOptions options;

    DiskANNSearchDefaults() = default;
    explicit DiskANNSearchDefaults(DiskANNSearchOptions options_) : options(options_) {}
};

/// Adapts a `DiskANNDiskIndexSearcher` to `IANNIndexSearcher`. The adapter translates the
/// FFI-style `search(query, k, ids, distances, ...)` into the interface's return-by-value
/// form. Per-query `search_list_size` / `beam_width` are forwarded from `ANNSearchOverrides`;
/// any field left at zero falls back to the default baked into the on-disk index.
class DiskANNIndexSearcherAdapter : public IANNIndexSearcher
{
public:
    DiskANNIndexSearcherAdapter(
        DiskANNDiskIndexSearcherPtr searcher_,
        DiskANNMetric metric_,
        size_t dim_);

    std::vector<ANNSearcherHit> search(
        const float * query,
        size_t query_dim,
        size_t k,
        const ANNSearchOverrides & overrides) const override;

    /// Delegates to the stateless free function `DiskANNComputeDistances` so callers
    /// that already hold a searcher can reach the kernel without crossing back through
    /// the factory. The searcher state itself is not used — the kernel is fully
    /// determined by `metric` and `dim`.
    void computeDistances(
        const float * query,
        size_t query_dim,
        const float * candidates,
        size_t n_candidates,
        float * out) const override;

private:
    DiskANNDiskIndexSearcherPtr searcher;
    DiskANNMetric metric;
    size_t dim;
};

}

#endif
