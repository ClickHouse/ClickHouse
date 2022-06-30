#pragma once

#include <Storages/ColumnsDescription.h>
#include <unordered_map>
#include <set>
#include <mutex>
#include <optional>

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
    /// Add new key with a schema with optional TTL
    void add(const String & key, const ColumnsDescription & columns, time_t ttl = 0);

    /// Add many keys with the same schema with optional TTL (usually used for globs)
    void addMany(const Strings & keys, const ColumnsDescription & columns, time_t ttl = 0);

    std::optional<ColumnsDescription> tryGet(const String & key, std::function<std::optional<time_t>()> get_last_mod_time = {});

private:
    void addUnlocked(const String & key, const ColumnsDescription & columns, time_t ttl);

    /// Check for expired TTLs.
    void clean();

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
