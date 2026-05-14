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

/// Per-part intermediate GROUP BY cache (`use_partial_aggregate_cache`). Key from `computePartialAggregateCacheQueryHash`
/// (`query_hash`, `table_uuid`, `part_name`, `part_mutation_version`). `part_mutation_version` comes from the data part, not from parsing `part_name`.
/// Merge/mutation yields a new part identity and thus a new key; stale entries expire by eviction. `SYSTEM DROP AGGREGATE CACHE` clears explicitly.
/// `get` at plan (`ReadFromMergeTree`) and execution (`AggregatingTransform`); skip execution `get` when `PartialAggregateInfo::skip_execution_time_cache_lookup`.
/// `put` after a miss in `AggregatingTransform::flushPartialAggregateMissBuffers`. `PartialAggregateCacheHits`/`Misses` count every `get`.
class PartialAggregateCache
{
public:
    struct Key
    {
        /// Hash of the aggregation query (`computePartialAggregateCacheQueryHash`, includes grouping-set tail when applicable).
        IASTHash query_hash;

        /// Table identity.
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

    /// Store intermediate states for a part after a cache miss was aggregated.
    void put(const Key & key, Block partial_aggregate);

    /// Clear all entries
    void clear();

    /// Get cache statistics
    size_t sizeInBytes() const;
    size_t count() const;

    /// Update cache configuration
    void setMaxSizeInBytes(size_t max_size_in_bytes);
    void setMaxEntries(size_t max_entries);

    /// For debugging and system tables
    std::vector<Cache::KeyMapped> dump() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("PartialAggregateCache");
};

using PartialAggregateCachePtr = std::shared_ptr<PartialAggregateCache>;

}
