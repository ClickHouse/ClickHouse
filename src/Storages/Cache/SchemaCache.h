#pragma once

#include <Storages/ColumnsDescription.h>
#include <unordered_map>
#include <mutex>
#include <ctime>
#include <optional>
#include <limits>

#include <Common/logger_useful.h>


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

/// Cache that stores columns description by some string key. It's used in schema inference.
/// It supports TTL for keys. Before each action it looks for expired TTls and removes
/// corresponding keys from cache. After each access to a key in cache it's TTL resumes,
/// so a key will be removed by TTL only if it was not accessed during this TTL.
/// It also supports keys invalidations by last modification time. If last modification time
/// is provided and last modification happened after a key was added to the cache, this key
/// will be removed from cache.
class SchemaCache
{
public:
    void add(const String & key, const ColumnsDescription & columns, time_t ttl = 0)
    {
        std::lock_guard lock(mutex);
        clean();
        addUnlocked(key, columns, ttl);
    }

    void addMany(const Strings & keys, const ColumnsDescription & columns, time_t ttl = 0)
    {
        std::lock_guard lock(mutex);
        clean();
        for (const auto & key : keys)
            addUnlocked(key, columns, ttl);
    }

    std::optional<ColumnsDescription> tryGet(const String & key, std::function<std::optional<time_t>()> get_last_mod_time = {})
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

    /// Check if this cache contains provided key.
    bool has(const String & key)
    {
        std::lock_guard lock(mutex);
        clean();
        return data.contains(key);
    }

private:
    void addUnlocked(const String & key, const ColumnsDescription & columns, time_t ttl)
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

    /// Check for expired TTLs.
    void clean()
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

    struct SchemaInfo
    {
        ColumnsDescription columns;
        time_t registration_time;
        time_t ttl;
        time_t valid_until;
    };

    std::unordered_map<String, SchemaInfo> data;
    /// Special queue for checking expired TTLs. It contains pairs
    /// (expiration time, key) sorted in ascending order.
    std::set<std::pair<time_t, String>> expiration_queue;
    std::mutex mutex;
};

}
