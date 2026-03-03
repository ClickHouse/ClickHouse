#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/SharedMutex.h>
#include <absl/container/flat_hash_map.h>

#include <mutex>
#include <shared_mutex>

namespace CurrentMetrics
{
    extern const Metric SerializationCacheBytes;
    extern const Metric SerializationCacheCount;
}

namespace DB
{

namespace SerializationObjectPool
{

struct Pool
{
    SharedMutex mutex;
    absl::flat_hash_map<UInt128, std::weak_ptr<const ISerialization>> map;
};

/// Intentionally leaked to avoid static destruction order issues: the custom
/// shared_ptr deleters reference the pool, but those deleters can fire from
/// any thread (including during thread_local / static destruction of caches
/// such as DataTypesCache or ColumnObject's getDynamicSerialization).  If the
/// pool were a regular static it could already be destroyed at that point.
Pool & getPool()
{
    static Pool * pool = new Pool;
    return *pool;
}

SerializationPtr getOrCreate(UInt128 key, SerializationUniquePtr && serialization)
{
    auto & pool = getPool();

    {
        std::shared_lock read_lock(pool.mutex);
        auto it = pool.map.find(key);
        if (it != pool.map.end())
        {
            if (auto res = it->second.lock())
                return res;
        }
    }

    std::unique_lock write_lock(pool.mutex);
    auto [it, inserted] = pool.map.emplace(key, std::weak_ptr<const ISerialization>());
    if (!inserted)
    {
        if (auto res = it->second.lock())
            return res;
    }

    CurrentMetrics::add(CurrentMetrics::SerializationCacheCount);
    CurrentMetrics::set(CurrentMetrics::SerializationCacheBytes, pool.map.capacity());

    const ISerialization * raw = serialization.release();
    SerializationPtr ret
    (
        raw,
        [k = std::move(key)](const ISerialization * ptr)
        {
            auto & p = getPool();
            {
                std::unique_lock lock(p.mutex);
                auto map_it = p.map.find(k);
                if (map_it != p.map.end() && map_it->second.expired())
                {
                    p.map.erase(map_it);
                    CurrentMetrics::sub(CurrentMetrics::SerializationCacheCount);
                    CurrentMetrics::set(CurrentMetrics::SerializationCacheBytes, p.map.capacity());
                }
            }
            delete ptr;
        }
    );

    it->second = ret;
    return ret;
}
}

}
