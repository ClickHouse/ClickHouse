#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event UniqueKeyBitmapLoadMicroseconds;
    extern const Event UniqueKeyBitmapUpdates;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeBitmapStore::MergeTreeBitmapStore(DeleteBitmapCachePtr cache_)
    : cache(std::move(cache_))
{
}

MergeTreeBitmapStore::MergeTreeBitmapStore(
    const MergeTreeData & data, DeleteBitmapCachePtr cache_)
    : cache(std::move(cache_))
    , resolution_data(&data)
{
}

std::vector<BitmapVersion> MergeTreeBitmapStore::getSnapshotCSNs(
    const IDataPartStorage & storage,
    const std::string & part_id)
{
    {
        std::shared_lock lock(csns_mutex);
        auto it = csns_per_part.find(part_id);
        if (it != csns_per_part.end())
            return it->second;
    }

    auto files = DeleteBitmapFileOps::enumerateFiles(storage);
    std::vector<BitmapVersion> csns;
    csns.reserve(files.size());
    for (const auto & f : files)
        csns.push_back(f.version);
    std::sort(csns.begin(), csns.end());

    /// `try_emplace` resolves concurrent populates of the same part_id:
    /// first publish wins, later arrivals get the existing entry.
    std::unique_lock lock(csns_mutex);
    auto [it, _inserted] = csns_per_part.try_emplace(part_id, std::move(csns));
    return it->second;
}

void MergeTreeBitmapStore::installBitmap(
    IDataPartStorage & storage,
    const std::string & part_id,
    const std::string & part_name,
    BitmapVersion csn,
    const DeleteBitmap & bitmap)
{
    /// CSN 0 is the "no bitmap" sentinel `readBitmap` returns for an empty selection, so a real
    /// bitmap may never be installed at version 0.
    if (csn == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "UNIQUE KEY: bitmap version (csn) must be non-zero for part {}; 0 is the no-bitmap sentinel",
            part_name);

    /// Hold the lock across the whole install so the monotonicity check, the durable write, and the
    /// in-memory index update are one critical section — a concurrent reader (shared lock) observes
    /// either the pre- or post-install state, never a half-state where the file is on disk but the
    /// index doesn't list it (or vice versa).
    ///
    /// On a cold index (no entry for `part_id`, e.g. after a restart), self-load the part's on-disk
    /// versions before checking monotonicity — symmetric with `readBitmap`/`getSnapshotCSNs`. Without
    /// this, an empty list silently bypasses the monotonicity guard and drops the on-disk history. A
    /// warm entry is the in-memory authority (install/gc/remove keep it in sync), so disk is read
    /// only when the entry is absent, not on every install.
    ///
    /// TODO(unique-key): the lock spans writeBitmapToStorage (disk I/O), serializing the store-wide
    /// index during the write. Acceptable now (installs serialized per partition, small/infrequent
    /// writes, no production caller), but could be narrowed later — reserve the csn slot under the
    /// lock, write outside it, then publish — once a concurrent workload justifies it.
    std::unique_lock lock(csns_mutex);
    auto map_it = csns_per_part.find(part_id);
    if (map_it == csns_per_part.end())
    {
        /// Mirror `getSnapshotCSNs`'s enumeration inline — calling it would re-acquire `csns_mutex`
        /// (SharedMutex, non-reentrant) and deadlock.
        auto files = DeleteBitmapFileOps::enumerateFiles(storage);
        std::vector<BitmapVersion> on_disk;
        on_disk.reserve(files.size());
        for (const auto & f : files)
            on_disk.push_back(f.version);
        std::sort(on_disk.begin(), on_disk.end());
        map_it = csns_per_part.emplace(part_id, std::move(on_disk)).first;
    }
    auto & csns = map_it->second;
    if (!csns.empty() && csn <= csns.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Bitmap version {} for part {} must be strictly greater "
            "than the latest installed version {}",
            csn, part_name, csns.back());

    DeleteBitmapFileOps::writeBitmapToStorage(storage, csn, bitmap, part_name);
    ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapUpdates);

    csns.push_back(csn); /// csn > csns.back() (checked above) keeps the list sorted ascending
}

std::pair<UniqueKeyTxn::ConstDeleteBitmapPtr, BitmapVersion> MergeTreeBitmapStore::readBitmap(
    const IDataPartStorage & storage,
    BitmapVersion snapshot_csn,
    const std::string & part_id)
{
    auto csns = getSnapshotCSNs(storage, part_id);

    auto it = std::upper_bound(csns.begin(), csns.end(), snapshot_csn);
    if (it == csns.begin())
        return {std::make_shared<DeleteBitmap>(), 0};
    const BitmapVersion chosen_csn = *(--it);

    if (!cache)
    {
        UniqueKeyTxn::ConstDeleteBitmapPtr bm = DeleteBitmapFileOps::readBitmapFromStorage(storage, chosen_csn, part_id);
        return {std::move(bm), chosen_csn};
    }

    const auto key = DeleteBitmapCache::makeKey(part_id, chosen_csn);
    /// `UniqueKeyBitmapLoadMicroseconds` is cache-miss-only by contract.
    auto [ptr, _loaded] = cache->getOrSet(key, [&]()
    {
        Stopwatch load_watch;
        auto bm = DeleteBitmapFileOps::readBitmapFromStorage(storage, chosen_csn, part_id);
        ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapLoadMicroseconds, load_watch.elapsedMicroseconds());
        return bm;
    });
    UniqueKeyTxn::ConstDeleteBitmapPtr immutable = std::move(ptr);
    return {std::move(immutable), chosen_csn};
}

size_t MergeTreeBitmapStore::gcObsoleteBitmaps(
    IDataPartStorage & storage,
    const std::string & part_id,
    BitmapVersion committed_csn,
    BitmapVersion oldest_snapshot_csn)
{
    /// Caller must serialize this with `installBitmap` (and `dropPart`) for the same part under the
    /// per-partition UK mutex: both mutate the part's on-disk files and the in-memory CSN index, and
    /// a gc that unlinks a version while an install is mid-flight could otherwise leave the index
    /// listing a removed version.
    auto csns = getSnapshotCSNs(storage, part_id);

    std::vector<BitmapVersion> committed;
    committed.reserve(csns.size());
    for (auto v : csns)
        if (v <= committed_csn)
            committed.push_back(v);

    std::vector<BitmapVersion> to_remove;
    for (size_t i = 0; i + 1 < committed.size(); ++i)
        if (committed[i + 1] <= oldest_snapshot_csn)
            to_remove.push_back(committed[i]);

    if (to_remove.empty())
        return 0;

    std::vector<BitmapVersion> removed;
    removed.reserve(to_remove.size());

    /// Reconcile the in-memory index with exactly the versions whose files were unlinked, on every
    /// exit. If a removal throws partway, csns_per_part must still match disk — otherwise a later
    /// readBitmap could select a version whose file is gone and throw.
    SCOPE_EXIT({
        if (removed.empty())
            return;
        std::unique_lock lock(csns_mutex);
        auto map_it = csns_per_part.find(part_id);
        if (map_it == csns_per_part.end())
            return;
        auto & cached_csns = map_it->second;
        cached_csns.erase(
            std::remove_if(cached_csns.begin(), cached_csns.end(),
                           [&](BitmapVersion v)
                           {
                               return std::find(removed.begin(), removed.end(), v) != removed.end();
                           }),
            cached_csns.end());
    });

    for (auto v : to_remove)
    {
        storage.removeFileIfExists(DeleteBitmap::fileNameForCSN(v));
        removed.push_back(v);
        if (cache)
            cache->remove(DeleteBitmapCache::makeKey(part_id, v));
        LOG_TRACE(getLogger("MergeTreeBitmapStore"),
                  "UNIQUE KEY: removed obsolete delete bitmap delete_bitmap_{}.rbm (committed={}, oldest_snapshot={})",
                  v, committed_csn, oldest_snapshot_csn);
    }

    return removed.size();
}

void MergeTreeBitmapStore::removeBitmap(
    IDataPartStorage & storage,
    const std::string & part_id,
    BitmapVersion csn)
{
    /// Reconcile the in-memory index on every exit once the file is unlinked: if cache
    /// eviction (or anything after the unlink) throws, `csns_per_part` must still match
    /// disk — otherwise a later `readBitmap` could select `csn`, whose file is gone, and
    /// throw. Mirrors the SCOPE_EXIT reconciliation in `gcObsoleteBitmaps`.
    bool unlinked = false;
    SCOPE_EXIT({
        if (!unlinked)
            return;
        std::unique_lock lock(csns_mutex);
        auto it = csns_per_part.find(part_id);
        if (it == csns_per_part.end())
            return;
        auto & csns = it->second;
        csns.erase(std::remove(csns.begin(), csns.end(), csn), csns.end());
    });

    /// Unlink the file (idempotent — `removeFileIfExists` no-ops a missing file).
    storage.removeFileIfExists(DeleteBitmap::fileNameForCSN(csn));
    unlinked = true;

    /// Invalidate the cache entry so a later reader does not serve a removed version.
    if (cache)
        cache->remove(DeleteBitmapCache::makeKey(part_id, csn));

    LOG_TRACE(getLogger("MergeTreeBitmapStore"),
              "UNIQUE KEY: removed delete bitmap sidecar delete_bitmap_{}.rbm for part '{}'", csn, part_id);
}

void MergeTreeBitmapStore::dropPart(const std::string & part_id)
{
    /// Caller must serialize this with `installBitmap` / `gcObsoleteBitmaps` for the same part under
    /// the per-partition UK mutex (see `installBitmap`): all three mutate the part's index state.
    {
        std::unique_lock lock(csns_mutex);
        csns_per_part.erase(part_id);
    }

    /// Evict the cache by part identity, not by the in-memory version list: `installBitmap`
    /// invalidates that list, so a drop right after an install would otherwise evict nothing and
    /// leave stale bitmaps that could alias a reused `disk:path` identity. Per-part removal mirrors
    /// `VectorSimilarityIndexCache::removeEntriesFromCache`.
    ///
    /// A read that missed the cache and is mid-`getOrSet` can still reinsert its bitmap after this
    /// returns; that in-flight-load race is a property of `CacheBase` shared by every per-part
    /// load-through cache (MarkCache, PrimaryIndexCache, VectorSimilarityIndexCache) and is left to
    /// CacheBase, not worked around here.
    if (cache)
        cache->removeEntriesForPart(part_id);
}

MergeTreeBitmapStore::ResolvedPart
MergeTreeBitmapStore::resolvePart(const UniqueKeyTxn::PartName & part_name) const
{
    if (!resolution_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MergeTreeBitmapStore: PartName-keyed bitmap access requires a resolution "
            "context (data + partition_id); this store was built cache-only");

    /// Single indexed lookup (`data_parts_by_info.find`, O(log P)). Part names are
    /// globally unique and encode the partition, so the global index needs no
    /// partition scoping. Active resolves to the live part; Outdated is also
    /// accepted because the sink can stage supersession bitmaps for parts that a
    /// merge Outdated between probe and commit — the old part's storage is still
    /// on disk until the cleaner runs, and writing the bitmap there is a harmless
    /// no-op (no reader observes an Outdated part). Returning a null target would
    /// crash via `installBitmap`'s LOGICAL_ERROR.
    if (auto p = resolution_data->getPartIfExists(
            part_name,
            {MergeTreeData::DataPartState::Active, MergeTreeData::DataPartState::Outdated}))
    {
        ResolvedPart resolved;
        /// `IDataPartStorage` is logically const on the part; the bitmap
        /// sidecar dir is legitimately mutable bookkeeping.
        resolved.storage = const_cast<IDataPartStorage *>(&p->getDataPartStorage());
        /// Stable cache key (part UUID, or `disk:path` fallback) so the
        /// process-wide `DeleteBitmapCache` is keyed identically to every
        /// other reader of this part's bitmaps.
        resolved.cache_identity = p->getDeleteBitmapCacheIdentity();
        return resolved;
    }

    /// Truly absent (deleting or never existed) — empty handle. `installBitmap`
    /// raises `LOGICAL_ERROR` on null storage; `removeBitmap` / `readBitmap`
    /// tolerate it.
    return {};
}

namespace
{
    /// Cache identity for the storage-level forward: the part's
    /// `getDeleteBitmapCacheIdentity` when resolution supplied one, else the
    /// `PartName` (single-part contexts that don't track a UUID).
    std::string cacheIdentityOf(
        const std::string & resolved_identity, const UniqueKeyTxn::PartName & part)
    {
        return resolved_identity.empty() ? part : resolved_identity;
    }
}

std::pair<UniqueKeyTxn::ConstDeleteBitmapPtr, UniqueKeyTxn::CSN>
MergeTreeBitmapStore::readBitmap(const UniqueKeyTxn::PartName & part, UniqueKeyTxn::CSN snapshot_csn)
{
    auto resolved = resolvePart(part);
    if (!resolved.storage)
        /// Unresolved part → miss sentinel (empty NON-NULL bitmap at csn 0).
        /// Callers MUST NOT branch on null — an empty bitmap is "no deletions".
        return {std::make_shared<DeleteBitmap>(), 0};

    return readBitmap(*resolved.storage, snapshot_csn, cacheIdentityOf(resolved.cache_identity, part));
}

void MergeTreeBitmapStore::installBitmap(
    const UniqueKeyTxn::PartName & target, UniqueKeyTxn::CSN csn, const DeleteBitmap & bitmap)
{
    auto resolved = resolvePart(target);
    if (!resolved.storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MergeTreeBitmapStore::installBitmap: resolver returned null storage for part '{}'", target);

    installBitmap(*resolved.storage, cacheIdentityOf(resolved.cache_identity, target), target, csn, bitmap);
}

void MergeTreeBitmapStore::removeBitmap(const UniqueKeyTxn::PartName & target, UniqueKeyTxn::CSN csn)
{
    auto resolved = resolvePart(target);
    /// A null target (part already gone from the active+outdated set) is
    /// tolerated: nothing to unlink, no index/cache entry to clear.
    if (!resolved.storage)
        return;

    removeBitmap(*resolved.storage, cacheIdentityOf(resolved.cache_identity, target), csn);
}

}
