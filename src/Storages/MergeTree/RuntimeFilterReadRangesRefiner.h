#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/StorageInMemoryMetadata.h>

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
    RuntimeFilterReadRangesRefiner(StorageMetadataPtr metadata_snapshot_, ContextPtr context_, String key_column_name_);
    ~RuntimeFilterReadRangesRefiner() override;

    void setFilter(const RuntimeFilterConstPtr & filter);

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override;

private:
    const StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    const String key_column_name;

    /// Written once by setFilter before any refine call (guaranteed by the gating edge).
    bool drop_all = false;
    std::shared_ptr<const KeyCondition> condition;
};

}
