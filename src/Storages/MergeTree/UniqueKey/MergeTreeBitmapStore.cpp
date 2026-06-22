#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <IO/ReadSettings.h>
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

namespace
{
    std::shared_ptr<DeleteBitmap> readFromStorage(
        const IDataPartStorage & storage,
        const std::string & file_name)
    {
        ReadSettings read_settings;
        auto buf = storage.readFile(file_name, read_settings, /*read_hint=*/{});
        auto deserialized = DeleteBitmap::deserialize(*buf);
        return std::shared_ptr<DeleteBitmap>(deserialized.release());
    }
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
    const IMergeTreeDataPart & part,
    BitmapVersion csn,
    const DeleteBitmap & bitmap)
{
    installBitmap(
        const_cast<IDataPartStorage &>(part.getDataPartStorage()),
        part.getDeleteBitmapCacheIdentity(),
        part.name,
        csn,
        bitmap);
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

    /// The store is the in-memory authority for a part's installed versions; no disk read is needed
    /// to manage the index. Hold the lock across the whole install so the monotonicity check, the
    /// durable write, and the in-memory index update are one critical section — a concurrent reader
    /// (shared lock) observes either the pre- or post-install state, never a half-state where the
    /// file is on disk but the index doesn't list it (or vice versa).
    ///
    /// Pre-existing on-disk versions are expected to be loaded into `csns_per_part` at part-load by
    /// the write-integration slice; install does not enumerate disk to rediscover them.
    ///
    /// TODO(unique-key): the lock spans writeBitmapToStorage (disk I/O), serializing the store-wide
    /// index during the write. Acceptable now (installs serialized per partition, small/infrequent
    /// writes, no production caller), but could be narrowed later — reserve the csn slot under the
    /// lock, write outside it, then publish — once a concurrent workload justifies it.
    std::unique_lock lock(csns_mutex);
    auto & csns = csns_per_part[part_id];
    if (!csns.empty() && csn <= csns.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Bitmap version {} for part {} must be strictly greater "
            "than the latest installed version {}",
            csn, part_name, csns.back());

    DeleteBitmapFileOps::writeBitmapToStorage(storage, csn, bitmap, part_name);
    ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapUpdates);

    csns.push_back(csn); /// csn > csns.back() (checked above) keeps the list sorted ascending
}

std::pair<std::shared_ptr<const DeleteBitmap>, BitmapVersion> MergeTreeBitmapStore::readBitmap(
    const IDataPartStorage & storage,
    BitmapVersion snapshot_csn,
    const std::string & part_id)
{
    auto csns = getSnapshotCSNs(storage, part_id);

    auto it = std::upper_bound(csns.begin(), csns.end(), snapshot_csn);
    if (it == csns.begin())
        return {std::make_shared<DeleteBitmap>(), 0};
    const BitmapVersion chosen_csn = *(--it);

    const String file_name = DeleteBitmap::fileNameForCSN(chosen_csn);
    if (!cache)
    {
        std::shared_ptr<const DeleteBitmap> bm = readFromStorage(storage, file_name);
        return {std::move(bm), chosen_csn};
    }

    const auto key = DeleteBitmapCache::makeKey(part_id, chosen_csn);
    /// `UniqueKeyBitmapLoadMicroseconds` is cache-miss-only by contract.
    auto [ptr, _loaded] = cache->getOrSet(key, [&]()
    {
        Stopwatch load_watch;
        auto bm = readFromStorage(storage, file_name);
        ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapLoadMicroseconds, load_watch.elapsedMicroseconds());
        return bm;
    });
    std::shared_ptr<const DeleteBitmap> immutable = std::move(ptr);
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

}
