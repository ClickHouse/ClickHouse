#pragma once

#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

struct MergeTreeIndexBuildContext;
using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

/// Prunes whole marks whose rows are all filtered out by a projection index bitmap
/// (see `MergeTreeProjectionIndexReader`).
///
/// Without a refiner the same bitmap is applied only during reading (`MergeTreeReaderIndex`):
/// fully filtered granules are skipped and the remaining rows are filtered inside the reading
/// chain, but the read task for a pruned granule still exists and its readers are still set up.
/// The refiner applies the granule-level part of the bitmap at task-cut time in the read pool,
/// so fully pruned ranges never become read tasks. Row-level filtering of the surviving
/// granules still happens in `MergeTreeReaderIndex`.
///
/// The first `refine` call for a part builds the bitmap (reads the projection), which is the
/// same work the first reading thread for the part would have done; subsequent calls reuse the
/// per-part cached result from `MergeTreeIndexReadResultPool`.
class ProjectionIndexReadRangesRefiner : public IMergeTreeReadRangesRefiner
{
public:
    ProjectionIndexReadRangesRefiner(MergeTreeIndexBuildContextPtr index_build_context_, StorageMetadataPtr metadata_snapshot_);

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override;

private:
    const MergeTreeIndexBuildContextPtr index_build_context;
    const StorageMetadataPtr metadata_snapshot;
};

}
