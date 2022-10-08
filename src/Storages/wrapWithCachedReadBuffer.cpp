#include <Storages/wrapWithCachedReadBuffer.h>

#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Storages/defineRemoteStoragesCache.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace
{
    class ModificationTimeCache
    {
    public:
        using Key = UInt128;

        void removeModificationTimeIfExists(Key key)
        {
            std::lock_guard lock(mutex);

            auto it = last_modification_time_cache.find(key);
            if (it == last_modification_time_cache.end())
                return;

            last_modification_time_cache.erase(it);
        }

        bool updateModificationTimeIfChanged(Key key, time_t last_modification_time)
        {
            std::lock_guard lock(mutex);

            time_t & cached_modification_time = last_modification_time_cache[key];

            if (cached_modification_time == 0 || cached_modification_time < last_modification_time)
            {
                cached_modification_time = last_modification_time;
                return true;
            }

            return false;
        }

    private:
        std::mutex mutex;
        std::unordered_map<Key, time_t> last_modification_time_cache TSA_GUARDED_BY(mutex);
    };
}


static ModificationTimeCache modification_time_cache;

std::unique_ptr<ReadBufferFromFileBase> wrapWithCachedReadBuffer(
    ImplementationBufferCreator impl_creator,
    ModificationTimeGetter modification_time_getter,
    const std::string & object_path,
    size_t object_size,
    const ReadSettings & read_settings)
{
    auto cache_data = FileCacheFactory::instance().tryGetByName(REMOTE_TABLE_ENGINES_CACHE_NAME);
    if (!cache_data)
    {
        /// Cache was not initiazed becuase it is not defined in config.
        return nullptr;
    }

    auto cache = cache_data->cache;
    auto cache_key = cache->hash(object_path);
    auto query_id = CurrentThread::isInitialized() ? std::string(CurrentThread::getQueryId()) : "";
    auto * log = &Poco::Logger::get("wrapWithCachedReadBuffer");

    const std::optional<time_t> last_modification_time = modification_time_getter();
    if (!last_modification_time)
    {
        LOG_WARNING(
            log,
            "Cache is enabled for remote storage, but it cannot be used "
            "because failed to find out object modification time");

        /// If we failed to find out modification time, we cannot validate cache.
        /// Choose not to use cache in this case.
        return nullptr;
    }

    bool modification_time_changed = modification_time_cache.updateModificationTimeIfChanged(cache_key.key, *last_modification_time);
    if (modification_time_changed)
    {
        LOG_TEST(log, "Modification time changes for file `{}` (new time: {})", object_path, *last_modification_time);
        /// FIXME: it will remove even if the file segments are currently used by someone.
        cache->removeIfExists(cache_key);
    }

    auto on_key_eviction_func = [key = cache_key.key]
    {
        modification_time_cache.removeModificationTimeIfExists(key);
    };

    auto buf = std::make_unique<CachedOnDiskReadBufferFromFile>(
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

    return buf;
}

}
