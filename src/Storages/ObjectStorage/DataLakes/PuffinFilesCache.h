#pragma once

#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <base/types.h>

#include <optional>

namespace ProfileEvents
{
extern const Event PuffinFilesCacheHits;
extern const Event PuffinFilesCacheMisses;
extern const Event PuffinFilesCacheWeightLost;
}

namespace DB
{

struct PuffinFilesCacheKey
{
    String file_path;
    String etag;
    Int64 content_offset = 0;
    Int64 content_size_in_bytes = 0;

    bool operator==(const PuffinFilesCacheKey & other) const;
};

struct PuffinFilesCacheKeyHash
{
    size_t operator()(const PuffinFilesCacheKey & key) const;
};

struct PuffinFilesCacheCell : private boost::noncopyable
{
    DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows;
    UInt64 memory_bytes = 0;

    explicit PuffinFilesCacheCell(DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows_);

private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;

    static UInt64 calculateMemorySize(const DataLakeObjectMetadata::ExcludedRowsPtr & excluded_rows_);
};

struct PuffinFilesCacheWeightFunction
{
    size_t operator()(const PuffinFilesCacheCell & cell) const;
};

/// Cache for parsed content loaded from Puffin files (deletion vectors today, indexes later).
class PuffinFilesCache : public CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>;

    PuffinFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

    static std::optional<PuffinFilesCacheKey> tryCreateKey(
        const String & file_path,
        const String & etag,
        Int64 content_offset,
        Int64 content_size_in_bytes);

    template <typename LoadFunc>
    DataLakeObjectMetadata::ExcludedRowsPtr getOrSetDeletionVector(const PuffinFilesCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto excluded_rows = load_fn();
            LOG_TRACE(
                log,
                "Loaded puffin deletion vector into cache for {} | {} at offset {} length {}",
                key.file_path,
                key.etag,
                key.content_offset,
                key.content_size_in_bytes);
            return std::make_shared<PuffinFilesCacheCell>(std::move(excluded_rows));
        };

        auto result = Base::getOrSet(key, load_fn_wrapper);
        if (result.second)
        {
            LOG_TRACE(log, "Puffin files cache miss for {} | {} at offset {} length {}", key.file_path, key.etag, key.content_offset, key.content_size_in_bytes);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheMisses);
        }
        else
        {
            LOG_TRACE(log, "Puffin files cache hit for {} | {} at offset {} length {}", key.file_path, key.etag, key.content_offset, key.content_size_in_bytes);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheHits);
        }

        return result.first->excluded_rows;
    }

private:
    LoggerPtr log;

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using PuffinFilesCachePtr = std::shared_ptr<PuffinFilesCache>;

}
