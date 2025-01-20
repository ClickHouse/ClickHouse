#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <mutex>

namespace DB
{

/// The cache of columns deserialization prefixes.
/// Used during reading from MergeTree Wide part to avoid reading
/// the same prefixes multiple times.
class DeserializationPrefixesCache
{
public:
    /// If not set, lock the mutex, deserialize prefix and return it.
    /// If set, just return the prefix.
    std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> getOrSet(const std::function<std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>()> & read_prefixes)
    {
        if (is_set)
            return clonePrefixes();

        std::unique_lock lock(mutex);
        if (is_set)
            return clonePrefixes();

        prefixes = read_prefixes();
        is_set = true;
        return clonePrefixes();
    }

    /// Prefixes can store and update some state during deserialization, so we should always return cloned states.
    std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> clonePrefixes() const
    {
        std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> cloned;
        cloned.reserve(prefixes->size());
        for (const auto & [name, prefix] : *prefixes)
            cloned[name] = prefix ? prefix->clone() : nullptr;
        return cloned;
    }

private:
    std::atomic_bool is_set = false;
    std::mutex mutex;
    std::optional<std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>> prefixes;
};

using DeserializationPrefixesCachePtr = std::shared_ptr<DeserializationPrefixesCache>;

}
