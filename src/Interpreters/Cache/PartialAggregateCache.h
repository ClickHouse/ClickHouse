#pragma once

#include <Core/Block.h>
#include <Core/UUID.h>
#include <Parsers/IASTHash.h>
#include <Common/CacheBase.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <optional>

namespace DB
{

/// Cache infrastructure for partial aggregate states per MergeTree part (`use_partial_aggregate_cache`).
/// Key: `query_hash` (aggregation query identity), plus `table_uuid`, `part_name`, `part_mutation_version`.
/// Entries with `table_uuid == UUIDHelpers::Nil` are intentionally ignored to avoid cross-table key collisions for non-UUID databases.
/// `PartialAggregateCacheHits`/`Misses` are updated on `get` only when `table_uuid` is non-`Nil`.
class PartialAggregateCache
{
public:
    struct Key
    {
        /// Hash of the aggregation query identity.
        IASTHash query_hash;

        /// Table identity. Must be non-`Nil` for cache participation.
        UUID table_uuid;

        /// Part identity (`name` + authoritative mutation counter from the data part, not inferred by parsing `part_name`).
        String part_name;
        UInt64 part_mutation_version;

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        /// Intermediate aggregate states (not finalized).
        Block partial_aggregate;

        /// Deep-size estimate used by cache eviction accounting.
        size_t weight_in_bytes = 0;

        /// When was this entry created
        std::chrono::time_point<std::chrono::system_clock> created_at;
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

private:
    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

public:
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

    PartialAggregateCache(size_t max_size_in_bytes, size_t max_entries);

    /// Lookup by key; updates `PartialAggregateCacheHits` / `PartialAggregateCacheMisses`.
    std::optional<Block> get(const Key & key);

    /// Store intermediate aggregate states for a part key.
    void put(const Key & key, Block partial_aggregate);

    /// Clear all entries
    void clear();

    /// Get cache statistics
    size_t sizeInBytes() const;
    size_t count() const;

    /// Update cache configuration
    void setMaxSizeInBytes(size_t max_size_in_bytes);
    void setMaxEntries(size_t max_entries);

    /// For introspection and debugging.
    std::vector<Cache::KeyMapped> dump() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("PartialAggregateCache");
};

using PartialAggregateCachePtr = std::shared_ptr<PartialAggregateCache>;

}
