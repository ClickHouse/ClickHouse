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
    std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>
    getOrSet(const std::function<std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>()> & read_prefixes);

    /// Prefixes can store and update some state during deserialization, so we should always return cloned states.
    std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr> clonePrefixes() const;

    /// Enumerate the original prefix states retained in the cache into the ownership validator without
    /// cloning them. Readers operate on clones (see `clonePrefixes`) that share the same column
    /// references (e.g. a `LowCardinality` `global_dictionary`) with these originals, so the cached
    /// originals are extra live holders that the validator must count; otherwise a broken reference
    /// count on a reader-local clone could go unnoticed on prefix-cached reads. See
    /// `ColumnsOwnershipValidator` and https://github.com/ClickHouse/ClickHouse/issues/105626.
    /// A no-op in release builds.
    void addToOwnershipValidator(ColumnsOwnershipValidator & validator) const;

private:
    std::atomic_bool is_set = false;
    std::mutex mutex;
    std::optional<std::unordered_map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>> prefixes;
};

using DeserializationPrefixesCachePtr = std::shared_ptr<DeserializationPrefixesCache>;

}
