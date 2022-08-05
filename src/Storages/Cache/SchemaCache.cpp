#include <Storages/Cache/SchemaCache.h>
#include <Common/ProfileEvents.h>
#include <ctime>

namespace ProfileEvents
{
    extern const Event SchemaInferenceCacheHits;
    extern const Event SchemaInferenceCacheMisses;
    extern const Event SchemaInferenceCacheEvictions;
    extern const Event SchemaInferenceCacheInvalidations;
}

namespace DB
{

SchemaCache::SchemaCache(size_t max_elements_) : max_elements(max_elements_)
{
}

void SchemaCache::add(const String & key, const ColumnsDescription & columns)
{
    std::lock_guard lock(mutex);
    addUnlocked(key, columns);
}


void SchemaCache::addMany(const Strings & keys, const ColumnsDescription & columns)
{
    std::lock_guard lock(mutex);
    for (const auto & key : keys)
        addUnlocked(key, columns);
}

void SchemaCache::addUnlocked(const String & key, const ColumnsDescription & columns)
{
    /// Do nothing if this key is already in cache;
    if (data.contains(key))
        return;

    time_t now = std::time(nullptr);
    auto it = queue.insert(queue.end(), key);
    data[key] = {SchemaInfo{columns, now}, it};
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

std::optional<ColumnsDescription> SchemaCache::tryGet(const String & key, LastModificationTimeGetter get_last_mod_time)
{
    std::lock_guard lock(mutex);
    auto it = data.find(key);
    if (it == data.end())
    {
        ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheMisses);
        return std::nullopt;
    }

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

    ProfileEvents::increment(ProfileEvents::SchemaInferenceCacheHits);
    return schema_info.columns;
}

void SchemaCache::clear()
{
    std::lock_guard lock(mutex);
    data.clear();
    queue.clear();
}

std::unordered_map<String, SchemaCache::SchemaInfo> SchemaCache::getAll()
{
    std::lock_guard lock(mutex);
    std::unordered_map<String, SchemaCache::SchemaInfo> result;
    for (const auto & [key, value] : data)
        result[key] = value.schema_info;

    return result;
}

}
