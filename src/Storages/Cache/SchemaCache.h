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

    struct SchemaInfo
    {
        ColumnsDescription columns;
        time_t registration_time;
    };

    using LastModificationTimeGetter = std::function<std::optional<time_t>()>;

    /// Add new key with a schema
    void add(const String & key, const ColumnsDescription & columns);

    /// Add many keys with the same schema (usually used for globs)
    void addMany(const Strings & keys, const ColumnsDescription & columns);

    std::optional<ColumnsDescription> tryGet(const String & key, LastModificationTimeGetter get_last_mod_time = {});

    void clear();

    std::unordered_map<String, SchemaInfo> getAll();

private:
    void addUnlocked(const String & key, const ColumnsDescription & columns);
    void checkOverflow();

    using Queue = std::list<String>;
    using QueueIterator = Queue::iterator;

    struct Cell
    {
        SchemaInfo schema_info;
        QueueIterator iterator;
    };

    Queue queue;
    std::unordered_map<String, Cell> data;

    size_t max_elements;
    std::mutex mutex;
};

}
