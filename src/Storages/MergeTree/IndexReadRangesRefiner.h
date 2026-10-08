#pragma once

#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

struct MergeTreeIndexBuildContext;
using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

/// Prunes whole marks whose rows are all filtered out by indexes applied
/// at data-read time: skip indexes and the projection index bitmap.
///
/// Without a refiner the same index read results are applied only during reading,
/// but the read task for a pruned granule still exists and its readers are still set up.
/// The refiner applies the granule-level part of the index read results at task-cut time
/// in the read pool, so fully pruned ranges never become read tasks.
/// It helps to avoid creating unnecessary read buffers and starting prefetches.
/// Row-level filtering of the surviving granules still happens in `MergeTreeReaderIndex`.
///
/// The first `refine` call for a part builds the index read results (reads the skip indexes
/// and/or the projection), which is the same work the first reading thread for the part would
/// have done; subsequent calls reuse the per-part cached result from `MergeTreeIndexReadResultPool`.
class IndexReadRangesRefiner : public IMergeTreeReadRangesRefiner
{
public:
    IndexReadRangesRefiner(MergeTreeIndexBuildContextPtr index_build_context_, StorageMetadataPtr metadata_snapshot_);

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override;

private:
    const MergeTreeIndexBuildContextPtr index_build_context;
    const StorageMetadataPtr metadata_snapshot;
};

}
