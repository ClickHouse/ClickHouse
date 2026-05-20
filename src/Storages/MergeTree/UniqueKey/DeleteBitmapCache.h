#pragma once

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Core/UUID.h>
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

/// Process-wide cache of deserialized `DeleteBitmap` objects, keyed by
/// (part UUID or storage UUID + relative part name hash) ⊕ bitmap version.
///
/// We deliberately do NOT key by `(part_storage_path + version)` string —
/// ClickHouse already has plenty of cases where different part names alias the
/// same rows after rename (ATTACH PARTITION, projections); using the part's
/// stable UUID + the bitmap version avoids aliasing bugs and keeps the
/// key size to `UInt128` (compatible with the idiomatic `UInt128TrivialHash`
/// used by `MarkCache` / `PrimaryIndexCache`).
///
/// The key encoding is done by `DeleteBitmapCache::makeKey(part_id, version)`.
class DeleteBitmapCache : public CacheBase<UInt128, DeleteBitmap, UInt128TrivialHash, DeleteBitmapWeightFunction>
{
private:
    using Base = CacheBase<UInt128, DeleteBitmap, UInt128TrivialHash, DeleteBitmapWeightFunction>;

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

    /// Derive the cache key from a part-identifying string (typically the
    /// part's UUID-as-string, with a fallback to disk:path when the UUID
    /// is nil — caller's choice) and the bitmap version.
    ///
    /// Collision model: SipHash-128 of `part_id` mixed with `version`.
    /// 128 bits is ample headroom for process-lifetime cache keys.
    static UInt128 makeKey(const String & part_id, BitmapVersion version)
    {
        SipHash hash;
        hash.update(part_id.size());
        hash.update(part_id.data(), part_id.size());
        hash.update(version);
        return hash.get128();
    }
};

using DeleteBitmapCachePtr = std::shared_ptr<DeleteBitmapCache>;

}
