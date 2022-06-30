#include <Storages/Cache/SchemaCache.h>
#include <Common/ProfileEvents.h>
#include <ctime>

namespace ProfileEvents
{
    extern const Event SchemaInferenceCacheHits;
    extern const Event SchemaInferenceCacheMisses;
    extern const Event SchemaInferenceCacheTTLExpirations;
    extern const Event SchemaInferenceCacheTTLUpdates;
    extern const Event SchemaInferenceCacheInvalidations;
}

namespace DB
{

void SchemaCache::add(const String & key, const ColumnsDescription & columns, time_t ttl)
{
    std::lock_guard lock(mutex);
    clean();
    addUnlocked(key, columns, ttl);
}


void SchemaCache::addMany(const Strings & keys, const ColumnsDescription & columns, time_t ttl)
{
    std::lock_guard lock(mutex);
    clean();
    for (const auto & key : keys)
        addUnlocked(key, columns, ttl);
}

void SchemaCache::addUnlocked(const String & key, const ColumnsDescription & columns, time_t ttl)
{
    /// Do nothing if this key is already in cache;
    if (data.contains(key))
        return;
    time_t now = std::time(nullptr);
    time_t valid_until = now + ttl;
    data[key] = SchemaInfo{columns, now, ttl, valid_until};
    if (ttl)
        expiration_queue.insert({valid_until, key});
}

std::optional<ColumnsDescription> SchemaCache::tryGet(const String & key, std::function<std::optional<time_t>()> get_last_mod_time)
{
    std::lock_guard lock(mutex);
    clean();
    auto it = data.find(key);
    if (it == data.end())
    {
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheMisses);
        return std::nullopt;
    }

    auto & schema_info = it->second;
    if (get_last_mod_time)
    {
        /// It's important to call get_last_mod_time only if we have key in cache,
        /// because this function can do some heavy operations.
        auto last_mod_time = get_last_mod_time();
        /// If get_last_mod_time function was provided but it returned nullopt, it means that
        /// it failed to get last modification time, so we cannot safely use value from cache.
        if (!last_mod_time)
            return std::nullopt;

        if (*last_mod_time >= schema_info.registration_time)
        {
            /// Object was modified after it was added in cache.
            /// So, stored value is no more valid and we should remove it.
            ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheInvalidations);
            /// If this key had TTL, we should remove it from expiration queue.
            if (schema_info.ttl)
                expiration_queue.erase({schema_info.valid_until, key});
            data.erase(key);
            return std::nullopt;
        }
    }

    if (schema_info.ttl)
    {
        /// Current value in cache is valid and we can resume it's TTL by updating it's expiration time.
        /// We will extract current value from the expiration queue, modify it and insert back to the queue.
        time_t now = std::time(nullptr);
        auto jt = expiration_queue.find({schema_info.valid_until, key});
        auto node = expiration_queue.extract(jt);
        schema_info.valid_until = now + schema_info.ttl;
        node.value().first = schema_info.valid_until;
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheTTLUpdates);
        expiration_queue.insert(std::move(node));
    }

    ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheHits);
    return schema_info.columns;
}

void SchemaCache::clean()
{
    time_t now = std::time(nullptr);
    auto it = expiration_queue.begin();
    /// Queue is sorted by time, so we need to check only the first
    /// values that are less than current time.
    while (it != expiration_queue.end() && it->first < now)
    {
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheTTLExpirations);
        data.erase(it->second);
        ++it;
    }
    expiration_queue.erase(expiration_queue.begin(), it);
}

}
