#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <mutex>

namespace DB
{

class KeyCondition;

/// Prunes mark ranges with the runtime filters collected from the build side of a JOIN:
/// the recorded exact key values (or the [min, max] envelope) of every filter are turned
/// into one ANDed condition on the primary key and applied to every cut through
/// `markRangesFromPKRange`.
///
/// The filters are picked up from the runtime filter lookup by `setFilter`, triggered by the
/// build-side completion seal (the pipeline edge guarantees this happens before any cut of
/// the gated side). Until then the refiner keeps the ranges unchanged, and so does a filter
/// which recorded nothing usable; a filter with an empty key set drops everything.
class RuntimeFilterReadRangesRefiner : public IMergeTreeReadRangesRefiner
{
public:
    /// The descriptors name the probe-side join key columns (covering a prefix of the
    /// primary key) and the filters built for them, as referenced by the pushed-down
    /// `__applyFilter` conjuncts.
    RuntimeFilterReadRangesRefiner(
        StorageMetadataPtr metadata_snapshot_, ContextPtr context_, std::vector<RuntimeFilterIndexAnalysisDescriptor> descriptors_);
    ~RuntimeFilterReadRangesRefiner() override;

    /// Idempotent and thread-safe: the seal is copied to every gated reading stream, so this
    /// is called once per stream with the same filter and only the first call takes effect.
    void setFilter(const RuntimeFilterConstPtr & filter);

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override;

private:
    void setFilterImpl(const RuntimeFilterConstPtr & filter);

    const StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    const std::vector<RuntimeFilterIndexAnalysisDescriptor> descriptors;

    /// Written once by setFilter before any refine call (guaranteed by the gating edge).
    std::once_flag set_filter_once;
    std::shared_ptr<const KeyCondition> condition;
};

}
