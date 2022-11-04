#include <Storages/wrapWithCachedReadBuffer.h>

#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Storages/defineRemoteStoragesCache.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace
{
    struct ModificationTimeCache
    {
        using Key = FileCache::Key;

        void removeModificationTimeIfExists(const Key & key)
        {
            std::lock_guard lock(mutex);

            auto it = last_modification_time_cache.find(key);
            if (it == last_modification_time_cache.end())
                return;

            LOG_TEST(log, "Removing key `{}` from modification time cache", key.toString());
            last_modification_time_cache.erase(it);
        }

        bool updateModificationTimeIfChanged(const Key & key, time_t last_modification_time)
        {
            std::lock_guard lock(mutex);
            auto [it, inserted] = last_modification_time_cache.emplace(key, last_modification_time);

            if (inserted)
            {
                LOG_TEST(
                    log,
                    "Modification time updated for file `{}` (new time: {})",
                    key.toString(), last_modification_time);

                return true;
            }

            auto & cached_modification_time = it->second;
            if (cached_modification_time != last_modification_time)
            {
                LOG_TEST(
                    log,
                    "Modification time changed for file `{}` (new time: {}, previous time: {})",
                    key.toString(), last_modification_time, cached_modification_time);

                cached_modification_time = last_modification_time;
                return true;
            }

            return false;
        }

        std::mutex mutex;
        std::unordered_map<Key, time_t> last_modification_time_cache TSA_GUARDED_BY(mutex);
        Poco::Logger * log = &Poco::Logger::get("ModificationTimeCache");
    };
}


ModificationTimeCache & getModificationTimeCache()
{
    static ModificationTimeCache modification_time_cache;
    return modification_time_cache;
}

std::unique_ptr<ReadBufferFromFileBase> wrapWithCachedReadBuffer(
    ImplementationBufferCreator impl_creator,
    const std::string & object_path,
    size_t object_size,
    time_t object_modification_time,
    const ReadSettings & read_settings)
{
    auto cache_data = FileCacheFactory::instance().tryGetByName(REMOTE_TABLE_ENGINES_CACHE_NAME);
    if (!cache_data) /// Cache was not initiazed becuase it is not defined in config.
        return nullptr;

    auto cache = cache_data->cache;
    auto cache_key = cache->hash(object_path);
    auto query_id = CurrentThread::isInitialized() ? std::string(CurrentThread::getQueryId()) : "";

    bool modification_time_changed = getModificationTimeCache().updateModificationTimeIfChanged(cache_key, object_modification_time);
    if (modification_time_changed)
    {
        LOG_TEST(
            &Poco::Logger::get("wrapeWithCachedReadBuffer"),
            "Removing `{}` from cache, because modification time changed",
            cache_key.toString());

        CacheKeyRemoveSettings settings{ .call_key_removal_callback = false };
        /// FIXME: it will remove even if the file segments are currently used by someone.
        cache->removeIfExists(cache_key, settings);
    }

    auto on_key_eviction_func = [=] { getModificationTimeCache().removeModificationTimeIfExists(cache_key); };

    return std::make_unique<CachedOnDiskReadBufferFromFile>(
        object_path,
        cache_key,
        cache,
        std::move(impl_creator),
        read_settings,
        query_id,
        object_size,
        /* allow_seeks */true,
        /* use_external_buffer */true,
        /* read_until_position */std::nullopt,
        std::move(on_key_eviction_func));
}

}
