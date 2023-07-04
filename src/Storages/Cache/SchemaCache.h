#pragma once

#include <Storages/ColumnsDescription.h>
#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>

namespace DB
{

const size_t DEFAULT_SCHEMA_CACHE_ELEMENTS = 4096;

/// Cache that stores columns description by some string key. It's used in schema inference.
/// It implements LRU semantic: after each access to a key in cache we move this key to
/// the end of the queue, if we reached the limit of maximum elements in the cache we
/// remove keys from the beginning of the queue.
/// It also supports keys invalidations by last modification time. If last modification time
/// is provided and last modification happened after a key was added to the cache, this key
/// will be removed from cache.
class SchemaCache
{
public:
    SchemaCache(size_t max_elements_);

    struct Key
    {
        String source;
        String format;
        String additional_format_info;

        bool operator==(const Key & other) const
        {
            return source == other.source && format == other.format && additional_format_info == other.additional_format_info;
        }
    };

    using Keys = std::vector<Key>;

    struct KeyHash
    {
        size_t operator()(const Key & key) const
        {
            return std::hash<String>()(key.source + key.format + key.additional_format_info);
        }
    };

    struct SchemaInfo
    {
        ColumnsDescription columns;
        time_t registration_time;
    };

    using LastModificationTimeGetter = std::function<std::optional<time_t>()>;

    /// Add new key with a schema
    void add(const Key & key, const ColumnsDescription & columns);

    /// Add many keys with the same schema (usually used for globs)
    void addMany(const Keys & keys, const ColumnsDescription & columns);

    std::optional<ColumnsDescription> tryGet(const Key & key, LastModificationTimeGetter get_last_mod_time = {});

    void clear();

    std::unordered_map<Key, SchemaInfo, SchemaCache::KeyHash> getAll();

private:
    void addUnlocked(const Key & key, const ColumnsDescription & columns);
    void checkOverflow();

    using Queue = std::list<Key>;
    using QueueIterator = Queue::iterator;

    struct Cell
    {
        SchemaInfo schema_info;
        QueueIterator iterator;
    };

    Queue queue;
    std::unordered_map<Key, Cell, KeyHash> data;

    size_t max_elements;
    std::mutex mutex;
};

}
