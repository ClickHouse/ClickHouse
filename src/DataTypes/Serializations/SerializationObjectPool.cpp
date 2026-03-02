#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Common/CurrentMetrics.h>
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

SerializationPtr getOrCreate(UInt128 key, SerializationUniquePtr && serialization)
{
    static std::shared_mutex columns_description_pool_mutex;
    static absl::flat_hash_map<UInt128, std::weak_ptr<const ISerialization>> columns_description_pool;

    {
        std::shared_lock<std::shared_mutex> read_lock(columns_description_pool_mutex);
        auto it = columns_description_pool.find(key);
        if (it != columns_description_pool.end())
        {
            if (auto res = it->second.lock())
                return res;
        }
    }

    std::unique_lock<std::shared_mutex> write_lock(columns_description_pool_mutex);
    auto [it, inserted] = columns_description_pool.emplace(key, std::weak_ptr<const ISerialization>());
    if (!inserted)
    {
        if (auto res = it->second.lock())
            return res;
    }

    CurrentMetrics::add(CurrentMetrics::SerializationCacheCount);
    CurrentMetrics::set(CurrentMetrics::SerializationCacheBytes, columns_description_pool.capacity());

    const ISerialization * raw = serialization.release();
    SerializationPtr ret
    (
        raw,
        [k = std::move(key)](const ISerialization * ptr)
        {
            {
                std::unique_lock<std::shared_mutex> lock(columns_description_pool_mutex);
                columns_description_pool.erase(k);
                CurrentMetrics::sub(CurrentMetrics::SerializationCacheCount);
                CurrentMetrics::set(CurrentMetrics::SerializationCacheBytes, columns_description_pool.capacity());
            }
            delete ptr;
        }
    );

    it->second = ret;
    return ret;
}
}

}

