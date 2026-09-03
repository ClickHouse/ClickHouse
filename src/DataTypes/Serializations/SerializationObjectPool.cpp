#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/SharedMutex.h>
#include <absl/container/flat_hash_map.h>

#include <atomic>
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

using SerializationMap = absl::flat_hash_map<UInt128, std::weak_ptr<const ISerialization>>;

/// The pool is sharded by key because every creation and every destruction of a pooled object
/// takes a write lock: a single lock turns any parallel path that builds serializations into a
/// serial one, and the readers waiting behind those writers burn CPU in the futex.
static constexpr size_t num_shards = 256;

/// Cache line aligned so that the shards' locks do not share one.
struct alignas(64) Shard
{
    SharedMutex mutex;
    SerializationMap map;
};

struct Pool
{
    Shard shards[num_shards];
    /// Sum of the shards' map storage, maintained by their writers so that the metric does not
    /// have to walk every shard.
    std::atomic<Int64> maps_allocated_bytes{0};
};

/// Intentionally leaked to avoid static destruction order issues: the custom
/// shared_ptr deleters reference the pool, but those deleters can fire from
/// any thread (including during thread_local / static destruction of caches
/// such as DataTypesCache or ColumnObject's getDynamicSerialization).  If the
/// pool were a regular static it could already be destroyed at that point.
static Pool & getPool()
{
    static Pool * pool = new Pool;
    return *pool;
}

static Shard & getShard(Pool & pool, UInt128 key)
{
    /// The key is a hash, so its low bits are distributed well enough to index a shard with.
    return pool.shards[static_cast<size_t>(static_cast<UInt64>(key) % num_shards)];
}

static Int64 mapAllocatedBytes(const SerializationMap & map)
{
    return static_cast<Int64>(sizeof(SerializationMap::value_type) * map.capacity());
}

SerializationPtr getOrCreate(UInt128 key, SerializationCreator creator)
{
    auto & pool = getPool();
    auto & shard = getShard(pool, key);
    {
        std::shared_lock read_lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it != shard.map.end())
            if (auto res = it->second.lock())
                return res;
    }

    /// Creating the serialization object must be outside of the critical section
    /// because there might be nested serializations.
    auto tmp = std::unique_ptr<const ISerialization>(creator());
    auto allocated_bytes = tmp->allocatedBytes();

    std::lock_guard write_lock(shard.mutex);
    const auto bytes_before = mapAllocatedBytes(shard.map);
    auto [it, inserted] = shard.map.emplace(key, std::weak_ptr<const ISerialization>());
    if (!inserted)
        if (auto res = it->second.lock())
            return res;

    CurrentMetrics::add(CurrentMetrics::SerializationCacheCount);
    CurrentMetrics::add(CurrentMetrics::SerializationCacheBytesInMemory, allocated_bytes);
    const auto delta = mapAllocatedBytes(shard.map) - bytes_before;
    CurrentMetrics::set(CurrentMetrics::SerializationCacheBytesInMemoryAllocated,
        pool.maps_allocated_bytes.fetch_add(delta) + delta + CurrentMetrics::get(CurrentMetrics::SerializationCacheBytesInMemory));

    SerializationPtr ret
    (
        tmp.release(),
        [k = std::move(key), b = allocated_bytes](const ISerialization * ptr)
        {
            auto & p = getPool();
            auto & s = getShard(p, k);
            {
                std::unique_lock lock(s.mutex);
                const auto bytes_before_erase = mapAllocatedBytes(s.map);
                auto map_it = s.map.find(k);
                if (map_it != s.map.end() && map_it->second.expired())
                    s.map.erase(map_it);

                CurrentMetrics::sub(CurrentMetrics::SerializationCacheCount);
                CurrentMetrics::sub(CurrentMetrics::SerializationCacheBytesInMemory, b);
                const auto erase_delta = mapAllocatedBytes(s.map) - bytes_before_erase;
                CurrentMetrics::set(CurrentMetrics::SerializationCacheBytesInMemoryAllocated,
                    p.maps_allocated_bytes.fetch_add(erase_delta) + erase_delta
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
