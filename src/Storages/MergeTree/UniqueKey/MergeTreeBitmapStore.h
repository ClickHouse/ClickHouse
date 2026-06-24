#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Common/SharedMutex.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class IDataPartStorage;
class MergeTreeData;

/// UNIQUE KEY — MergeTree-backed bitmap store. Bitmap files are named
/// `delete_bitmap_<csn>.rbm`; each commit allocates a global
/// per-partition csn and writes one cumulative bitmap per touched part
/// at that csn. Readers pin a `snapshot_csn` and pick the file with
/// `max csn ≤ snapshot_csn` per part.
///
/// Also serves as the Local implementation of the transaction layer's
/// `IBitmapStore` seam: the `PartName`-keyed overrides resolve a part name
/// to its `(storage, cache_identity)` over the `MergeTreeData` + partition_id
/// supplied to the resolution-context ctor, then forward to the storage-level
/// methods below. The cache-only ctor leaves the context unset; the
/// `PartName` overrides then throw `LOGICAL_ERROR`.
///
/// Holds a shared_ptr to the process-wide `DeleteBitmapCache` (owned by `Context`); null when
/// caching is disabled. Shared ownership avoids a dangling cache reference.
class MergeTreeBitmapStore : public UniqueKeyTxn::IBitmapStore
{
public:
    explicit MergeTreeBitmapStore(DeleteBitmapCachePtr cache_ = nullptr);

    /// Resolution-context ctor: the `PartName`-keyed `IBitmapStore` overrides
    /// resolve names over `data`'s active+outdated parts. `data` must outlive
    /// the store.
    MergeTreeBitmapStore(const MergeTreeData & data, DeleteBitmapCachePtr cache_);

    /// `IBitmapStore` (txn seam). Resolve `part`/`target` to its
    /// `(storage, cache_identity)` over the resolution context, then forward
    /// to the storage-level methods. Throw `LOGICAL_ERROR` if the store was
    /// built with the cache-only ctor (no resolution context).
    std::pair<UniqueKeyTxn::ConstDeleteBitmapPtr, UniqueKeyTxn::CSN>
        readBitmap(const UniqueKeyTxn::PartName & part, UniqueKeyTxn::CSN snapshot_csn) override;
    void installBitmap(const UniqueKeyTxn::PartName & target, UniqueKeyTxn::CSN csn, const DeleteBitmap & bitmap) override;
    void removeBitmap(const UniqueKeyTxn::PartName & target, UniqueKeyTxn::CSN csn) override;

    /// Install `bitmap` for `(storage, part_id)` at `csn`. Atomic. Caller has
    /// already computed the cumulative `prev_bitmap ∪ new_kills`, MUST hold the
    /// per-partition UK mutex, and has fsync'd the per-part manifest before this
    /// call. Throws `LOGICAL_ERROR` if `csn` is not strictly greater than every
    /// previously installed version for this part (monotonicity).
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
    std::pair<UniqueKeyTxn::ConstDeleteBitmapPtr, BitmapVersion> readBitmap(
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

    /// Precisely remove the single `(part_id, csn)` bitmap file, drop its
    /// cache entry, and erase that csn from the in-memory version index.
    /// Idempotent — a missing file/entry is not an error. Used by commit
    /// rollback and recovery's orphan tmp-scan to reclaim a bitmap named in
    /// an aborted commit's `bitmaps_created`. Caller must serialize this with
    /// `installBitmap` / `gcObsoleteBitmaps` / `dropPart` for the same part
    /// under the per-partition UK mutex (see `installBitmap`).
    void removeBitmap(
        IDataPartStorage & storage,
        const std::string & part_id,
        BitmapVersion csn);

private:
    DeleteBitmapCachePtr cache;

    /// Resolution context for the `PartName`-keyed `IBitmapStore` overrides.
    /// Unset under the cache-only ctor, in which case those overrides throw.
    /// Non-owning — the resolution-context ctor's caller keeps `data` alive
    /// longer than the store.
    const MergeTreeData * resolution_data = nullptr;

    /// Per-part handle a `PartName` resolves to over `resolution_data`:
    /// `storage` (null when the part is gone) + the part's
    /// `getDeleteBitmapCacheIdentity` (empty → caller falls back to the name).
    struct ResolvedPart
    {
        IDataPartStorage * storage = nullptr;
        std::string cache_identity;
    };
    ResolvedPart resolvePart(const UniqueKeyTxn::PartName & part_name) const;

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
