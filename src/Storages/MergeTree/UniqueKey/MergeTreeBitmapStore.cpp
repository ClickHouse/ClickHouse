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

MergeTreeBitmapStore::MergeTreeBitmapStore(DeleteBitmapCache * cache_)
    : cache(cache_)
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

std::vector<BitmapVersion> MergeTreeBitmapStore::snapshotCsns(
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
    const std::string part_id = part.getDeleteBitmapCacheIdentity();
    auto & storage = const_cast<IDataPartStorage &>(part.getDataPartStorage());

    /// Populate before the monotonicity check so a first-install on a
    /// never-seen part does not erase pre-existing on-disk versions
    /// from a prior run.
    (void)snapshotCsns(storage, part_id);

    /// Enforce strict monotonicity. The caller's per-partition UK mutex
    /// guarantees no concurrent installs on this part, so the
    /// shared-lock snapshot below is stable for the rest of this call.
    {
        std::shared_lock lock(csns_mutex);
        const auto & csns = csns_per_part[part_id];
        if (!csns.empty() && csn <= csns.back())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Bitmap version {} for part {} must be strictly greater "
                "than the latest installed version {}",
                csn, part.name, csns.back());
    }

    DeleteBitmapFileOps::writeBitmapToStorage(storage, csn, bitmap, part.name);
    ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapUpdates);

    std::unique_lock lock(csns_mutex);
    csns_per_part[part_id].push_back(csn);
    /// No cache invalidation: bitmaps are immutable by version; readers
    /// at `snapshot_version < csn` may still need the prior version.
}

std::pair<std::shared_ptr<const DeleteBitmap>, BitmapVersion> MergeTreeBitmapStore::readBitmap(
    const IDataPartStorage & storage,
    BitmapVersion snapshot_csn,
    const std::string & part_id)
{
    auto csns = snapshotCsns(storage, part_id);

    auto it = std::upper_bound(csns.begin(), csns.end(), snapshot_csn);
    if (it == csns.begin())
        return {std::make_shared<DeleteBitmap>(), 0};
    const BitmapVersion chosen_csn = *(--it);

    const String file_name = DeleteBitmap::fileNameForCsn(chosen_csn);
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
    auto csns = snapshotCsns(storage, part_id);

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

    for (auto v : to_remove)
    {
        storage.removeFileIfExists(DeleteBitmap::fileNameForCsn(v));
        if (cache)
            cache->remove(DeleteBitmapCache::makeKey(part_id, v));
        LOG_TRACE(getLogger("MergeTreeBitmapStore"),
                  "UNIQUE KEY: removed obsolete delete bitmap delete_bitmap_{}.rbm (committed={}, oldest_snapshot={})",
                  v, committed_csn, oldest_snapshot_csn);
    }

    std::unique_lock lock(csns_mutex);
    auto map_it = csns_per_part.find(part_id);
    if (map_it != csns_per_part.end())
    {
        auto & cached_csns = map_it->second;
        cached_csns.erase(
            std::remove_if(cached_csns.begin(), cached_csns.end(),
                           [&](BitmapVersion v)
                           {
                               return std::find(to_remove.begin(), to_remove.end(), v) != to_remove.end();
                           }),
            cached_csns.end());
    }

    return to_remove.size();
}

void MergeTreeBitmapStore::dropPart(const std::string & part_id)
{
    std::vector<BitmapVersion> csns_to_invalidate;
    {
        std::unique_lock lock(csns_mutex);
        auto it = csns_per_part.find(part_id);
        if (it == csns_per_part.end())
            return;
        csns_to_invalidate = std::move(it->second);
        csns_per_part.erase(it);
    }

    if (cache)
        for (auto v : csns_to_invalidate)
            cache->remove(DeleteBitmapCache::makeKey(part_id, v));
}

}
