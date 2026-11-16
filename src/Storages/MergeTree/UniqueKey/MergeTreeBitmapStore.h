#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Common/SharedMutex.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class IDataPartStorage;
class IMergeTreeDataPart;

/// UNIQUE KEY — MergeTree-backed bitmap store. Bitmap files are named
/// `delete_bitmap_<csn>.rbm`; each commit allocates a global
/// per-partition csn and writes one cumulative bitmap per touched part
/// at that csn. Readers pin a `snapshot_csn` and pick the file with
/// `max csn ≤ snapshot_csn` per part.
///
/// Holds a shared_ptr to the process-wide `DeleteBitmapCache` (owned by `Context`); null when
/// caching is disabled. Shared ownership avoids a dangling cache reference.
class MergeTreeBitmapStore
{
public:
    explicit MergeTreeBitmapStore(DeleteBitmapCachePtr cache_ = nullptr);

    /// Install `bitmap` for `part` at `csn`. Atomic. Caller has already
    /// computed the cumulative `prev_bitmap ∪ new_kills`. Caller MUST
    /// hold the per-partition UK mutex and has fsync'd the per-part
    /// manifest before this call. Throws `LOGICAL_ERROR` if `csn` is
    /// not strictly greater than every previously installed version
    /// for this part (monotonicity).
    void installBitmap(
        const IMergeTreeDataPart & part,
        BitmapVersion csn,
        const DeleteBitmap & bitmap);

    /// Storage-level overload. The `IMergeTreeDataPart` version forwards to
    /// this one after unpacking `part_id` and `part_name`. Exposed so unit
    /// tests can exercise the install path without constructing a real
    /// part. Same contract.
    void installBitmap(
        IDataPartStorage & storage,
        const std::string & part_id,
        const std::string & part_name,
        BitmapVersion csn,
        const DeleteBitmap & bitmap);

    /// Pick the bitmap visible at `snapshot_csn` for `(storage, part_id)`:
    /// the file with `max csn ≤ snapshot_csn`, or (empty non-null
    /// bitmap, 0) if none. The returned csn is the bitmap's csn.
    ///
    /// Returned pointer is `const`: cached bitmaps are shared across
    /// readers. Writers that need to build a new version copy first
    /// and mutate the copy.
    std::pair<std::shared_ptr<const DeleteBitmap>, BitmapVersion> readBitmap(
        const IDataPartStorage & storage,
        BitmapVersion snapshot_csn,
        const std::string & part_id);

    /// Among bitmaps on `(storage, part_id)` with `csn ≤ committed_csn`,
    /// drop each `V_b` whose adjacent successor `V_next` satisfies
    /// `V_next ≤ oldest_snapshot_csn`. The newest committed bitmap is
    /// kept (no `V_next`). Also drops the corresponding cache entries.
    /// Returns the number of files removed.
    size_t gcObsoleteBitmaps(
        IDataPartStorage & storage,
        const std::string & part_id,
        BitmapVersion committed_csn,
        BitmapVersion oldest_snapshot_csn);

    /// Drop in-memory state for `part_id` and invalidate its cache
    /// entries. Storage is not touched. Idempotent.
    void dropPart(const std::string & part_id);

private:
    DeleteBitmapCachePtr cache;

    /// Per-`part_id` sorted ascending csn list. Lazily populated on
    /// first access; updated by `installBitmap` and `gcObsoleteBitmaps`.
    ///
    /// TODO: convert to an LRU cache so stale entries (for parts that
    /// have left the active set without an explicit `dropPart` call)
    /// evict on size pressure instead of accumulating indefinitely. A
    /// missed-eviction is currently bounded but not auto-recovered.
    mutable SharedMutex csns_mutex;
    mutable std::unordered_map<std::string, std::vector<BitmapVersion>> csns_per_part;

    std::vector<BitmapVersion> getSnapshotCSNs(
        const IDataPartStorage & storage,
        const std::string & part_id);
};

}
