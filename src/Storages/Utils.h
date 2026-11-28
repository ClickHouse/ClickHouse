#pragma once

#include <Common/CurrentMetrics.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{
    struct StorageID;
    struct StorageSnapshot;
    using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

    std::vector<CurrentMetrics::Metric> getAttachedCountersForStorage(const StoragePtr & storage);

    /// The stage for a storage that reads `source` at `source_stage`, then converts its columns to those of `declared_snapshot`:
    /// `FetchColumns` when a conversion breaks the order and the read lands on a `Merge`, also behind wrappers that forward
    /// the read. A `Distributed` is not lowered, its `FetchColumns` read returns identifiers.
    QueryProcessingStage::Enum getStageForConvertingRead(
        QueryProcessingStage::Enum source_stage,
        const StoragePtr & source,
        const StorageSnapshotPtr & source_snapshot,
        const StorageSnapshotPtr & declared_snapshot);

    /// Throws if `getStageForConvertingRead` would lower `stage` for these columns.
    void checkStageForConvertingRead(
        QueryProcessingStage::Enum stage,
        const StoragePtr & source,
        const StorageSnapshotPtr & source_snapshot,
        const StorageSnapshotPtr & declared_snapshot,
        const StorageID & reader);
}
