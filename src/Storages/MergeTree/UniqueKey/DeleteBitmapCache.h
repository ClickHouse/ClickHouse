#pragma once

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>

#include <base/types.h>


namespace DB
{

/// Weight function: uses the bitmap's serialized-size estimate (see
/// `DeleteBitmap::memoryUsage`). Plus a small constant for map/ptr overhead.
struct DeleteBitmapWeightFunction
{
    static constexpr size_t DELETE_BITMAP_CACHE_OVERHEAD = 96;

    size_t operator()(const DeleteBitmap & bitmap) const
    {
        return bitmap.memoryUsage() + DELETE_BITMAP_CACHE_OVERHEAD;
    }
};

/// Cache key for a deserialized `DeleteBitmap`: the part-identity string plus the bitmap version.
///
/// `part_id` is the caller's stable cache identity — the part UUID, or the `disk:path` fallback
/// when the UUID is nil (see `IMergeTreeDataPart::getDeleteBitmapCacheIdentity`).
///
/// A stale entry cannot alias a later part that reuses the same path: `version` is a csn, csns are
/// allocated once and never reused, and a read only looks up a version the store's index holds for
/// that part. Entries a part leaves behind are LRU pollution, which the cache's size cap bounds.
struct DeleteBitmapCacheKey
{
    String part_id;
    BitmapVersion version;

    bool operator==(const DeleteBitmapCacheKey & other) const
    {
        return version == other.version && part_id == other.part_id;
    }
};

struct DeleteBitmapCacheKeyHash
{
    size_t operator()(const DeleteBitmapCacheKey & key) const
    {
        SipHash hash;
        hash.update(key.part_id.size());
        hash.update(key.part_id.data(), key.part_id.size());
        hash.update(key.version);
        return hash.get64();
    }
};

/// Process-wide cache of deserialized `DeleteBitmap` objects, keyed by part identity ⊕ version.
class DeleteBitmapCache : public CacheBase<DeleteBitmapCacheKey, DeleteBitmap, DeleteBitmapCacheKeyHash, DeleteBitmapWeightFunction>
{
private:
    using Base = CacheBase<DeleteBitmapCacheKey, DeleteBitmap, DeleteBitmapCacheKeyHash, DeleteBitmapWeightFunction>;

public:
    DeleteBitmapCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        double size_ratio)
        : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, /*max_count=*/0, size_ratio)
    {
    }

    static DeleteBitmapCacheKey makeKey(const String & part_id, BitmapVersion version)
    {
        return DeleteBitmapCacheKey{part_id, version};
    }

    /// Every version cached for one part, dropped together with the part. Without this a retired
    /// part's bitmaps stay resident until the size cap evicts them, competing with the live set.
    void removeEntriesForPart(const String & part_id)
    {
        Base::remove([&part_id](const Key & key, const MappedPtr &) { return key.part_id == part_id; });
    }
};

using DeleteBitmapCachePtr = std::shared_ptr<DeleteBitmapCache>;

}
