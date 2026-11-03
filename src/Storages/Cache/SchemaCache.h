#pragma once

#include <Storages/ColumnsDescription.h>
#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>

namespace DB
{

const size_t DEFAULT_SCHEMA_CACHE_ELEMENTS = 4096;

/// Cache that stores columns description and/or number of rows by some string key.
/// It's used in schema inference and fast count from format file.
/// It implements LRU semantic: after each access to a key in cache we move this key to
/// the end of the queue, if we reached the limit of maximum elements in the cache we
/// remove keys from the beginning of the queue.
/// It also supports keys invalidations by last modification time. If last modification time
/// is provided and last modification happened after a key was added to the cache, this key
/// will be removed from cache.
class SchemaCache
{
public:
    explicit SchemaCache(size_t max_elements_);

    struct Key
    {
        String source;
        String format;
        String additional_format_info;
        String schema_inference_mode;

        bool operator==(const Key & other) const
        {
            return source == other.source && format == other.format && additional_format_info == other.additional_format_info && schema_inference_mode == other.schema_inference_mode;
        }
    };

    using Keys = std::vector<Key>;

    struct KeyHash
    {
        size_t operator()(const Key & key) const
        {
            return std::hash<String>()(key.source + key.format + key.additional_format_info + key.schema_inference_mode);
        }
    };

    struct SchemaInfo
    {
        std::optional<ColumnsDescription> columns;
        std::optional<size_t> num_rows;
        time_t registration_time{};
        /// The resource the entry was recorded from, when the key names a group of resources rather than
        /// one of them (a Web failover group names several endpoints under one URL expression). The
        /// endpoints of a group can serve different contents under the same path and the same
        /// modification time, so an entry is only served for the resource it was read from.
        std::optional<String> resolved_url;
    };

    using LastModificationTimeGetter = std::function<std::optional<time_t>()>;
    /// The resource the caller is about to use, or nothing when it cannot tell. Called after
    /// `LastModificationTimeGetter`, which is what resolves it for a Web failover group. It runs under the
    /// cache lock, so it has to be cheap and must not do I/O.
    using ResolvedURLGetter = std::function<std::optional<String>()>;

    /// Add new key or update existing with a schema
    void addColumns(const Key & key, const ColumnsDescription & columns, const std::optional<String> & resolved_url = {});
    /// Add/update many keys with the same schema (usually used for globs)
    void addManyColumns(const Keys & keys, const ColumnsDescription & columns, const std::optional<String> & resolved_url = {});

    /// Add new key or update existing with number of rows
    void addNumRows(const Key & key, size_t num_rows, const std::optional<String> & resolved_url = {});

    std::optional<ColumnsDescription> tryGetColumns(
        const Key & key, LastModificationTimeGetter get_last_mod_time = {}, ResolvedURLGetter get_resolved_url = {});
    std::optional<size_t> tryGetNumRows(
        const Key & key, LastModificationTimeGetter get_last_mod_time = {}, ResolvedURLGetter get_resolved_url = {});

    void clear();

    std::unordered_map<Key, SchemaInfo, SchemaCache::KeyHash> getAll();

private:
    void addUnlocked(
        const Key & key,
        const std::optional<ColumnsDescription> & columns,
        std::optional<size_t> num_rows,
        const std::optional<String> & resolved_url);
    std::optional<SchemaInfo> tryGetImpl(
        const Key & key, LastModificationTimeGetter get_last_mod_time, ResolvedURLGetter get_resolved_url);
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
