#pragma once

#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <base/defines.h>
#include <base/types.h>

#include <memory>
#include <optional>

#include <roaring/roaring64map.hh>

namespace ProfileEvents
{
extern const Event PuffinFilesCacheHits;
extern const Event PuffinFilesCacheMisses;
extern const Event PuffinFilesCacheWeightLost;
}

namespace DB
{

/// Identity of one deletion-vector blob inside a Puffin file.
/// `storage_identity` separates backends that share a relative path.
/// `etag` is a strong content token (`ObjectMetadata::isEtagUsableAsCacheKey`); a weak or empty
/// etag is not a key, so those blobs are always read.
struct PuffinFilesCacheKey
{
    String storage_identity;
    String file_path;
    String etag;
    Int64 content_offset = 0;
    Int64 content_size_in_bytes = 0;

    bool operator==(const PuffinFilesCacheKey & other) const;

    /// Approximate bytes for key strings plus the key object (used in entry weight).
    UInt64 approximateMemoryBytes() const;
};

struct PuffinFilesCacheKeyHash
{
    size_t operator()(const PuffinFilesCacheKey & key) const;
};

struct PuffinFilesCacheCell : private boost::noncopyable
{
    std::shared_ptr<const roaring::Roaring64Map> bitmap;
    UInt64 memory_bytes = 0;

    PuffinFilesCacheCell(std::shared_ptr<const roaring::Roaring64Map> bitmap_, UInt64 key_memory_bytes_);

    static UInt64 calculateMemorySize(const roaring::Roaring64Map & bitmap_, UInt64 key_memory_bytes_);

private:
    /// Hash-map node, LRU node, and `shared_ptr` control block are not in the bitmap size.
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 256;
};

struct PuffinFilesCacheWeightFunction
{
    size_t operator()(const PuffinFilesCacheCell & cell) const;
};

/// Cache of parsed Iceberg deletion-vector bitmaps loaded from Puffin files.
/// Callers receive a clone, so mutating the returned bitmap cannot change the cached value.
class PuffinFilesCache : public CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>;

    PuffinFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

    /// `getName()://getDescription()/getObjectsNamespace()/getCommonKeyPrefix()`.
    static String makeStorageIdentity(const IObjectStorage & object_storage);

    /// Empty when `metadata` has no strong etag. Such blobs must be read, not cached.
    static std::optional<PuffinFilesCacheKey> tryCreateKey(
        const String & storage_identity,
        const String & file_path,
        const ObjectMetadata & metadata,
        Int64 content_offset,
        Int64 content_size_in_bytes);

    template <typename LoadFunc>
    std::unique_ptr<roaring::Roaring64Map> getOrSetDeletionVector(const PuffinFilesCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto bitmap = load_fn();
            chassert(bitmap);
            LOG_TRACE(
                log,
                "Loaded puffin deletion vector into cache for {} | {} | {} at offset {} length {}",
                key.storage_identity,
                key.file_path,
                key.etag,
                key.content_offset,
                key.content_size_in_bytes);
            return std::make_shared<PuffinFilesCacheCell>(
                std::shared_ptr<const roaring::Roaring64Map>(std::move(bitmap)), key.approximateMemoryBytes());
        };

        auto [cell, outcome] = Base::getOrSetWithOutcome(key, load_fn_wrapper);
        if (outcome == CacheGetOrSetOutcome::Hit)
        {
            LOG_TRACE(
                log,
                "Puffin files cache hit for {} | {} | {} at offset {} length {}",
                key.storage_identity,
                key.file_path,
                key.etag,
                key.content_offset,
                key.content_size_in_bytes);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheHits);
        }
        else
        {
            LOG_TRACE(
                log,
                "Puffin files cache miss for {} | {} | {} at offset {} length {}",
                key.storage_identity,
                key.file_path,
                key.etag,
                key.content_offset,
                key.content_size_in_bytes);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheMisses);
        }

        return cloneBitmap(*cell);
    }

private:
    static std::unique_ptr<roaring::Roaring64Map> cloneBitmap(const PuffinFilesCacheCell & cell);

    LoggerPtr log;

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using PuffinFilesCachePtr = std::shared_ptr<PuffinFilesCache>;

}
