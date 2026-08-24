#include <Storages/Cache/SchemaCache.h>
#include <Common/ProfileEvents.h>
#include <ctime>

namespace ProfileEvents
{
    extern const Event SchemaInferenceCacheHits;
    extern const Event SchemaInferenceCacheSchemaHits;
    extern const Event SchemaInferenceCacheNumRowsHits;
    extern const Event SchemaInferenceCacheMisses;
    extern const Event SchemaInferenceCacheSchemaMisses;
    extern const Event SchemaInferenceCacheNumRowsMisses;
    extern const Event SchemaInferenceCacheEvictions;
    extern const Event SchemaInferenceCacheInvalidations;
}

namespace DB
{

SchemaCache::SchemaCache(size_t max_elements_) : max_elements(max_elements_)
{
}

void SchemaCache::addColumns(const Key & key, const ColumnsDescription & columns)
{
    std::lock_guard lock(mutex);
    addUnlocked(key, columns, std::nullopt);
}


void SchemaCache::addManyColumns(const Keys & keys, const ColumnsDescription & columns)
{
    std::lock_guard lock(mutex);
    for (const auto & key : keys)
        addUnlocked(key, columns, std::nullopt);
}

void SchemaCache::addNumRows(const DB::SchemaCache::Key & key, size_t num_rows)
{
    std::lock_guard lock(mutex);
    addUnlocked(key, std::nullopt, num_rows);
}

void SchemaCache::addUnlocked(const Key & key, const std::optional<ColumnsDescription> & columns, std::optional<size_t> num_rows)
{
    /// Update columns/num_rows with new values if this key is already in cache.
    if (auto it = data.find(key); it != data.end())
    {
        if (columns)
            it->second.schema_info.columns = columns;
        if (num_rows)
            it->second.schema_info.num_rows = num_rows;
        return;
    }

    time_t now = std::time(nullptr);
    auto it = queue.insert(queue.end(), key);
    data[key] = {SchemaInfo{columns, num_rows, now}, it};
    checkOverflow();
}

void SchemaCache::checkOverflow()
{
    if (queue.size() <= max_elements)
        return;

    auto key = queue.front();
    data.erase(key);
    queue.pop_front();
    ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheEvictions);
}

std::optional<ColumnsDescription> SchemaCache::tryGetColumns(const DB::SchemaCache::Key & key, DB::SchemaCache::LastModificationTimeGetter get_last_mod_time)
{
    auto schema_info = tryGetImpl(key, get_last_mod_time);
    if (!schema_info)
        return std::nullopt;

    if (schema_info->columns)
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheSchemaHits);
    else
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheSchemaMisses);

    return schema_info->columns;
}

std::optional<size_t> SchemaCache::tryGetNumRows(const DB::SchemaCache::Key & key, DB::SchemaCache::LastModificationTimeGetter get_last_mod_time)
{
    auto schema_info = tryGetImpl(key, get_last_mod_time);
    if (!schema_info)
        return std::nullopt;

    if (schema_info->num_rows)
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheNumRowsHits);
    else
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheNumRowsMisses);

    return schema_info->num_rows;
}

std::optional<SchemaCache::SchemaInfo> SchemaCache::tryGetImpl(const Key & key, LastModificationTimeGetter get_last_mod_time)
{
    std::lock_guard lock(mutex);
    auto it = data.find(key);
    if (it == data.end())
    {
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheMisses);
        return std::nullopt;
    }

    ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheHits);

    auto & schema_info = it->second.schema_info;
    auto & queue_iterator = it->second.iterator;
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
            queue.erase(queue_iterator);
            data.erase(key);
            return std::nullopt;
        }
    }

    /// Move key to the end of queue.
    queue.splice(queue.end(), queue, queue_iterator);
    return schema_info;
}

void SchemaCache::clear()
{
    std::lock_guard lock(mutex);
    data.clear();
    queue.clear();
}

std::unordered_map<SchemaCache::Key, SchemaCache::SchemaInfo, SchemaCache::KeyHash> SchemaCache::getAll()
{
    std::lock_guard lock(mutex);
    std::unordered_map<Key, SchemaCache::SchemaInfo, SchemaCache::KeyHash> result;
    for (const auto & [key, value] : data)
        result[key] = value.schema_info;

    return result;
}

}
