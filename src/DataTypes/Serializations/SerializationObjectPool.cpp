#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/SharedMutex.h>
#include <absl/container/flat_hash_map.h>

#include <mutex>
#include <shared_mutex>

namespace CurrentMetrics
{
    extern const Metric SerializationCacheBytesInMemoryAllocated;
    extern const Metric SerializationCacheBytesInMemory;
    extern const Metric SerializationCacheCount;
}

namespace DB
{

namespace SerializationObjectPool
{

struct Pool
{
    SharedMutex mutex;
    using SerializationMap = absl::flat_hash_map<UInt128, std::weak_ptr<const ISerialization>>;
    SerializationMap map;
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

SerializationPtr getOrCreate(UInt128 key, SerializationCreator creator)
{
    auto & pool = getPool();
    {
        std::shared_lock read_lock(pool.mutex);
        auto it = pool.map.find(key);
        if (it != pool.map.end())
            if (auto res = it->second.lock())
                return res;
    }

    /// Creating the serialization object must be outside of the critical section
    /// because there might be nested serializations.
    auto tmp = std::unique_ptr<const ISerialization>(creator());
    auto allocated_bytes = tmp->allocatedBytes();

    std::lock_guard write_lock(pool.mutex);
    auto [it, inserted] = pool.map.emplace(key, std::weak_ptr<const ISerialization>());
    if (!inserted)
        if (auto res = it->second.lock())
            return res;

    CurrentMetrics::add(CurrentMetrics::SerializationCacheCount);
    CurrentMetrics::add(CurrentMetrics::SerializationCacheBytesInMemory, allocated_bytes);
    CurrentMetrics::set(CurrentMetrics::SerializationCacheBytesInMemoryAllocated,
        sizeof(typename Pool::SerializationMap::value_type) * pool.map.capacity()
        + CurrentMetrics::get(CurrentMetrics::SerializationCacheBytesInMemory));

    SerializationPtr ret
    (
        tmp.release(),
        [k = std::move(key), b = allocated_bytes](const ISerialization * ptr)
        {
            auto & p = getPool();
            {
                std::unique_lock lock(p.mutex);
                auto map_it = p.map.find(k);
                if (map_it != p.map.end() && map_it->second.expired())
                    p.map.erase(map_it);

                CurrentMetrics::sub(CurrentMetrics::SerializationCacheCount);
                CurrentMetrics::sub(CurrentMetrics::SerializationCacheBytesInMemory, b);
                CurrentMetrics::set(CurrentMetrics::SerializationCacheBytesInMemoryAllocated,
                    sizeof(typename Pool::SerializationMap::value_type) * p.map.capacity()
                    + CurrentMetrics::get(CurrentMetrics::SerializationCacheBytesInMemory));
            }
            delete ptr;
        }
    );

    it->second = ret;
    return ret;
}
}

}
