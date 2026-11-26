#include <Interpreters/Cache/EncryptionHeaderCache.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event EncryptionHeaderCacheHits;
    extern const Event EncryptionHeaderCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric EncryptionHeaderCacheBytes;
    extern const Metric EncryptionHeaderCacheEntries;
}

namespace DB
{

EncryptionHeaderCache::Key EncryptionHeaderCache::makeKey(const String & storage_path)
{
    SipHash hash;
    hash.update(storage_path);
    return hash.get128();
}

EncryptionHeaderCache::EncryptionHeaderCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, CurrentMetrics::EncryptionHeaderCacheBytes, CurrentMetrics::EncryptionHeaderCacheEntries, max_size_in_bytes, 0, size_ratio)
{
}

void EncryptionHeaderCache::write(const String & storage_path, HeaderBytes bytes)
{
    Key key = makeKey(storage_path);
    auto load_func = [&] { return std::make_shared<Entry>(std::move(bytes)); };
    auto [entry, inserted] = cache.getOrSet(key, load_func);
    LOG_TEST(logger, "{} entry for storage_path: {}, header_bytes: {}",
        inserted ? "Inserted" : "Reused", storage_path, entry->bytes.size());
}

std::optional<EncryptionHeaderCache::HeaderBytes> EncryptionHeaderCache::read(const String & storage_path)
{
    Key key = makeKey(storage_path);

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::EncryptionHeaderCacheHits);
        LOG_TEST(logger, "Read entry for storage_path: {}, header_bytes: {}", storage_path, entry->bytes.size());
        return {entry->bytes};
    }

    ProfileEvents::increment(ProfileEvents::EncryptionHeaderCacheMisses);
    LOG_TEST(logger, "Could not find entry for storage_path: {}", storage_path);
    return {};
}

EncryptionHeaderCache::HeaderBytes EncryptionHeaderCache::getOrSet(const String & storage_path, const std::function<HeaderBytes()> & load)
{
    Key key = makeKey(storage_path);
    bool loaded = false;
    auto load_func = [&]
    {
        auto entry = std::make_shared<Entry>(load());
        loaded = true;
        return entry;
    };
    auto entry = cache.getOrSet(key, load_func).first;
    ProfileEvents::increment(loaded ? ProfileEvents::EncryptionHeaderCacheMisses : ProfileEvents::EncryptionHeaderCacheHits);
    LOG_TEST(logger, "{} entry for storage_path: {}, header_bytes: {}", loaded ? "Loaded" : "Read", storage_path, entry->bytes.size());
    return entry->bytes;
}

void EncryptionHeaderCache::clear()
{
    cache.clear();
}

void EncryptionHeaderCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    cache.setMaxSizeInBytes(max_size_in_bytes);
}

size_t EncryptionHeaderCache::maxSizeInBytes() const
{
    return cache.maxSizeInBytes();
}

}
