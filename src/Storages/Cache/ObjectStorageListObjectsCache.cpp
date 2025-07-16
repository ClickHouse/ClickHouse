#include <Storages/Cache/ObjectStorageListObjectsCache.h>
#include <Common/TTLCachePolicy.h>
#include <Common/ProfileEvents.h>
#include <boost/functional/hash.hpp>

namespace ProfileEvents
{
extern const Event ObjectStorageListObjectsCacheHits;
extern const Event ObjectStorageListObjectsCacheMisses;
extern const Event ObjectStorageListObjectsCacheExactMatchHits;
extern const Event ObjectStorageListObjectsCachePrefixMatchHits;
}

namespace DB
{

template <typename Key, typename Mapped, typename HashFunction, typename WeightFunction, typename IsStaleFunction>
class ObjectStorageListObjectsCachePolicy : public TTLCachePolicy<Key, Mapped, HashFunction, WeightFunction, IsStaleFunction>
{
public:
    using BasePolicy = TTLCachePolicy<Key, Mapped, HashFunction, WeightFunction, IsStaleFunction>;
    using typename BasePolicy::MappedPtr;
    using typename BasePolicy::KeyMapped;
    using BasePolicy::cache;

    ObjectStorageListObjectsCachePolicy()
        : BasePolicy(CurrentMetrics::end(), CurrentMetrics::end(), std::make_unique<NoCachePolicyUserQuota>())
    {
    }

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        if (const auto it = cache.find(key); it != cache.end())
        {
            if (!IsStaleFunction()(it->first))
            {
                return std::make_optional<KeyMapped>({it->first, it->second});
            }
            // found a stale entry, remove it but don't return. We still want to perform the prefix matching search
            BasePolicy::remove(it->first);
        }

        if (const auto it = findBestMatchingPrefixAndRemoveExpiredEntries(key); it != cache.end())
        {
            return std::make_optional<KeyMapped>({it->first, it->second});
        }

        return std::nullopt;
    }

private:
    auto findBestMatchingPrefixAndRemoveExpiredEntries(Key key)
    {
        while (!key.prefix.empty())
        {
            if (const auto it = cache.find(key); it != cache.end())
            {
                if (IsStaleFunction()(it->first))
                {
                    BasePolicy::remove(it->first);
                }
                else
                {
                    return it;
                }
            }

            key.prefix.pop_back();
        }

        return cache.end();
    }
};

ObjectStorageListObjectsCache::Key::Key(
    const String & storage_description_,
    const String & bucket_,
    const String & prefix_,
    const std::chrono::steady_clock::time_point & expires_at_,
    std::optional<UUID> user_id_)
    : storage_description(storage_description_), bucket(bucket_), prefix(prefix_), expires_at(expires_at_), user_id(user_id_) {}

bool ObjectStorageListObjectsCache::Key::operator==(const Key & other) const
{
    return storage_description == other.storage_description && bucket == other.bucket && prefix == other.prefix;
}

size_t ObjectStorageListObjectsCache::KeyHasher::operator()(const Key & key) const
{
    std::size_t seed = 0;

    boost::hash_combine(seed, key.storage_description);
    boost::hash_combine(seed, key.bucket);
    boost::hash_combine(seed, key.prefix);

    return seed;
}

bool ObjectStorageListObjectsCache::IsStale::operator()(const Key & key) const
{
    return key.expires_at < std::chrono::steady_clock::now();
}

size_t ObjectStorageListObjectsCache::WeightFunction::operator()(const Value & value) const
{
    std::size_t weight = 0;

    for (const auto & object : value)
    {
        const auto object_metadata = object->metadata;
        weight += object->relative_path.capacity() + sizeof(object_metadata);

        // variable size
        if (object_metadata)
        {
            weight += object_metadata->etag.capacity();
            weight += object_metadata->attributes.size() * (sizeof(std::string) * 2);

            for (const auto & [k, v] : object_metadata->attributes)
            {
                weight += k.capacity() + v.capacity();
            }
        }
    }

    return weight;
}

ObjectStorageListObjectsCache::ObjectStorageListObjectsCache()
    : cache(std::make_unique<ObjectStorageListObjectsCachePolicy<Key, Value, KeyHasher, WeightFunction, IsStale>>())
{
}

void ObjectStorageListObjectsCache::set(
    const Key & key,
    const std::shared_ptr<Value> & value)
{
    auto key_with_ttl = key;
    key_with_ttl.expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(ttl_in_seconds);

    cache.set(key_with_ttl, value);
}

void ObjectStorageListObjectsCache::clear()
{
    cache.clear();
}

std::optional<ObjectStorageListObjectsCache::Value> ObjectStorageListObjectsCache::get(const Key & key, bool filter_by_prefix)
{
    const auto pair = cache.getWithKey(key);

    if (!pair)
    {
        ProfileEvents::increment(ProfileEvents::ObjectStorageListObjectsCacheMisses);
        return {};
    }

    ProfileEvents::increment(ProfileEvents::ObjectStorageListObjectsCacheHits);

    if (pair->key == key)
    {
        ProfileEvents::increment(ProfileEvents::ObjectStorageListObjectsCacheExactMatchHits);
        return *pair->mapped;
    }

    ProfileEvents::increment(ProfileEvents::ObjectStorageListObjectsCachePrefixMatchHits);

    if (!filter_by_prefix)
    {
        return *pair->mapped;
    }

    Value filtered_objects;

    filtered_objects.reserve(pair->mapped->size());

    for (const auto & object : *pair->mapped)
    {
        if (object->relative_path.starts_with(key.prefix))
        {
            filtered_objects.push_back(object);
        }
    }

    return filtered_objects;
}

void ObjectStorageListObjectsCache::setMaxSizeInBytes(std::size_t size_in_bytes_)
{
    cache.setMaxSizeInBytes(size_in_bytes_);
}

void ObjectStorageListObjectsCache::setMaxCount(std::size_t count)
{
    cache.setMaxCount(count);
}

void ObjectStorageListObjectsCache::setTTL(std::size_t ttl_in_seconds_)
{
    ttl_in_seconds = ttl_in_seconds_;
}

ObjectStorageListObjectsCache & ObjectStorageListObjectsCache::instance()
{
    static ObjectStorageListObjectsCache instance;
    return instance;
}

}
