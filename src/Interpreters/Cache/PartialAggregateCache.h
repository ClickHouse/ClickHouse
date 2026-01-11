#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Parsers/IASTHash.h>

#include <chrono>
#include <optional>

namespace DB
{

/// Cache for partial aggregation results at the MergeTree part level.
/// 
/// This cache stores intermediate aggregation results (partial aggregates) keyed by
/// (query_hash, part_name, part_mutation_version). Since MergeTree parts are immutable,
/// cached partial aggregates remain valid until the part is merged or mutated.
///
/// Use case: time-series queries that aggregate over append-only data. Historical parts
/// produce the same partial aggregates on every execution, so we cache and reuse them.
///
/// Enable with: SET use_partial_aggregate_cache = 1
class PartialAggregateCache
{
public:
    struct Key
    {
        /// Hash of the aggregation query (GROUP BY keys + aggregate functions)
        IASTHash query_hash;

        /// Part identification
        String part_name;
        UInt64 part_mutation_version;

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        /// The cached partial aggregate block
        Block partial_aggregate;

        /// When was this entry created
        std::chrono::time_point<std::chrono::system_clock> created_at;
    };

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

public:
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

    PartialAggregateCache(size_t max_size_in_bytes, size_t max_entries);

    /// Try to get a cached partial aggregate for the given key
    std::optional<Block> get(const Key & key);

    /// Store a partial aggregate in the cache
    void put(const Key & key, Block partial_aggregate);

    /// Clear all entries
    void clear();

    /// Get cache statistics
    size_t sizeInBytes() const;
    size_t count() const;

    /// Update cache configuration
    void setMaxSizeInBytes(size_t max_size_in_bytes);

    /// For debugging and system tables
    std::vector<Cache::KeyMapped> dump() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("PartialAggregateCache");
};

using PartialAggregateCachePtr = std::shared_ptr<PartialAggregateCache>;

}

