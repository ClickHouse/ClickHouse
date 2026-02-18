#pragma once

#include <DataTypes/Serializations/ISerialization.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

/// Pool for constant serialization objects.
/// Used to create them only once and share them between different columns.
class SerializationObjectPool
{
public:
    static SerializationObjectPool & instance()
    {
        static SerializationObjectPool cache;
        return cache;
    }

    SerializationPtr getOrCreate(const String & key, SerializationPtr && serialization)
    {
        SerializationPtr res;
        {
            std::lock_guard lock(mutex);
            res = cache.insert({key, std::move(serialization)}).first->second;
        }
        return res;
    }

    void remove(const String & key)
    {
        std::lock_guard lock(mutex);
        auto it = cache.find(key);
        /// use_count == 2 means: one in cache, one held by the object being destroyed
        if (it != cache.end() && it->second.use_count() == 2)
            cache.erase(it);
    }

private:
    SerializationObjectPool() = default;

    /// Unfortunately we have to use a recursive mutex here, because
    /// SerializationLowCardinality creates an inner dictionary Serialization
    /// that also uses this pool.
    mutable std::recursive_mutex mutex;
    std::unordered_map<String, SerializationPtr> cache;
};

}
