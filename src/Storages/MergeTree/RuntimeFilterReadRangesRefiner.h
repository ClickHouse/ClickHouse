#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <mutex>

namespace DB
{

class IRuntimeFilter;
using RuntimeFilterConstPtr = std::shared_ptr<const IRuntimeFilter>;

class KeyCondition;

/// Prunes mark ranges with a runtime filter collected from the build side of a JOIN:
/// the recorded exact key values are turned into a `key IN (...)` condition on the primary
/// key and applied to every cut through `markRangesFromPKRange`.
///
/// The filter is delivered by `setFilter` when the build side completes (in seal-gated
/// reading the pipeline edge guarantees this happens before any cut of the gated side).
/// Until then the refiner keeps the ranges unchanged. A ready filter with no recorded
/// values (e.g. overflowed into a bloom filter) also keeps the ranges unchanged; a filter
/// with an empty key set drops everything.
class RuntimeFilterReadRangesRefiner : public IMergeTreeReadRangesRefiner
{
public:
    /// The key column name and type describe the probe-side join key column the runtime
    /// filter was built for, as referenced by the pushed-down `__applyFilter` conjunct.
    RuntimeFilterReadRangesRefiner(
        StorageMetadataPtr metadata_snapshot_, ContextPtr context_, String key_column_name_, DataTypePtr key_column_type_);
    ~RuntimeFilterReadRangesRefiner() override;

    /// Idempotent and thread-safe: the seal is copied to every gated reading stream, so this
    /// is called once per stream with the same filter and only the first call takes effect.
    void setFilter(const RuntimeFilterConstPtr & filter);

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override;

private:
    void setFilterImpl(const RuntimeFilterConstPtr & filter);

    const StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    const String key_column_name;
    const DataTypePtr key_column_type;

    /// Written once by setFilter before any refine call (guaranteed by the gating edge).
    std::once_flag set_filter_once;
    std::shared_ptr<const KeyCondition> condition;
};

}
