#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Common/CacheLine.h>
#include <Common/CurrentMetrics.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <absl/container/flat_hash_map.h>

#include <array>
#include <mutex>

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

/// Sharded by key: the pool is consulted on every serialization construction
/// from every thread, and a single mutex was a measurable point of contention.
static constexpr size_t NUM_SHARDS = 64;
static_assert((NUM_SHARDS & (NUM_SHARDS - 1)) == 0, "NUM_SHARDS must be a power of two");

struct alignas(CH_CACHE_LINE_SIZE) Shard
{
    using SerializationMap = absl::flat_hash_map<UInt128, std::weak_ptr<const ISerialization>>;
    static constexpr size_t SLOT_SIZE = sizeof(SerializationMap::value_type);

    SharedMutex mutex;
    SerializationMap map TSA_GUARDED_BY(mutex);
};

using Pool = std::array<Shard, NUM_SHARDS>;

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

static Shard & getShard(UInt128 key)
{
    /// Keys are SipHash128 outputs, so any limb is uniformly distributed.
    return getPool()[key.items[UInt128::_impl::little(1)] & (NUM_SHARDS - 1)];
}

SerializationPtr getOrCreate(UInt128 key, SerializationCreator creator)
{
    auto & shard = getShard(key);
    {
        SharedLockGuard read_lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it != shard.map.end())
            if (auto res = it->second.lock())
                return res;
    }

    /// Creating the serialization object must be outside of the critical section
    /// because there might be nested serializations.
    auto tmp = std::unique_ptr<const ISerialization>(creator());
    auto allocated_bytes = static_cast<Int64>(tmp->allocatedBytes());

    std::lock_guard write_lock(shard.mutex);
    size_t capacity_before = shard.map.capacity();
    auto [it, inserted] = shard.map.emplace(key, std::weak_ptr<const ISerialization>());
    if (!inserted)
        if (auto res = it->second.lock())
            return res;

    /// Metrics are maintained incrementally rather than recomputed with `set`,
    /// because other shards update them concurrently. `erase` never shrinks the
    /// table, so capacity can only grow here, under this shard's write lock.
    Int64 capacity_delta = static_cast<Int64>((shard.map.capacity() - capacity_before) * Shard::SLOT_SIZE);
    CurrentMetrics::add(CurrentMetrics::SerializationCacheCount);
    CurrentMetrics::add(CurrentMetrics::SerializationCacheBytesInMemory, allocated_bytes);
    CurrentMetrics::add(CurrentMetrics::SerializationCacheBytesInMemoryAllocated, allocated_bytes + capacity_delta);

    SerializationPtr ret
    (
        tmp.release(),
        [&shard, k = key, b = allocated_bytes](const ISerialization * ptr)
        {
            {
                std::lock_guard lock(shard.mutex);
                /// Another thread may already have replaced the expired entry with a live object.
                auto map_it = shard.map.find(k);
                if (map_it != shard.map.end() && map_it->second.expired())
                    shard.map.erase(map_it);

                CurrentMetrics::sub(CurrentMetrics::SerializationCacheCount);
                CurrentMetrics::sub(CurrentMetrics::SerializationCacheBytesInMemory, b);
                CurrentMetrics::sub(CurrentMetrics::SerializationCacheBytesInMemoryAllocated, b);
            }
            delete ptr;
        }
    );

    it->second = ret;
    return ret;
}
}

}
