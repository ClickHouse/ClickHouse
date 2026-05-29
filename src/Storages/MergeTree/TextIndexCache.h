#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListSegment.h>

#include <absl/container/flat_hash_map.h>

#include <variant>

namespace ProfileEvents
{
    extern const Event TextIndexTokensCacheHits;
    extern const Event TextIndexTokensCacheMisses;
    extern const Event TextIndexHeaderCacheHits;
    extern const Event TextIndexHeaderCacheMisses;
    extern const Event TextIndexPostingsCacheHits;
    extern const Event TextIndexPostingsCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric TextIndexTokensCacheBytes;
    extern const Metric TextIndexTokensCacheCells;
    extern const Metric TextIndexHeaderCacheBytes;
    extern const Metric TextIndexHeaderCacheCells;
    extern const Metric TextIndexPostingsCacheBytes;
    extern const Metric TextIndexPostingsCacheCells;
}

namespace DB
{

/// Estimate of the memory usage (bytes) of a token info in cache
struct TextIndexTokensWeightFunction
{
    size_t operator()(const TokenPostingsInfo & token_info) const
    {
        return token_info.bytesAllocated();
    }
};

class TextIndexTokensCache : public CacheBase<UInt128, TokenPostingsInfo, UInt128TrivialHash, TextIndexTokensWeightFunction>
{
public:
    TextIndexTokensCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexTokensCacheBytes, CurrentMetrics::TextIndexTokensCacheCells, max_size_in_bytes, max_count, size_ratio)
    {
    }

    template <typename... Args>
    static UInt128 hash(Args... args)
    {
        SipHash hasher;
        (hasher.update(args),...);
        return hasher.get128();
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(UInt128 key, LoadFunc && load_func)
    {
        auto [cache_entry, cache_miss] = CacheBase::getOrSet(key, load_func);
        if (cache_miss)
            ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheHits);
        return std::move(cache_entry);
    }
};

/// Estimate of the memory usage (bytes) of a text index header in cache
struct TextIndexHeaderWeightFunction
{
    size_t operator()(const TextIndexHeader & header) const
    {
        return header.sparse_index.memoryUsageBytes();
    }
};

class TextIndexHeaderCache : public CacheBase<UInt128, TextIndexHeader, UInt128TrivialHash, TextIndexHeaderWeightFunction>
{
public:
    TextIndexHeaderCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexHeaderCacheBytes, CurrentMetrics::TextIndexHeaderCacheCells, max_size_in_bytes, max_count, size_ratio)
    {}

    template <typename... Args>
    static UInt128 hash(Args... args)
    {
        SipHash hasher;
        (hasher.update(args),...);
        return hasher.get128();
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(UInt128 key, LoadFunc && load_func)
    {
        auto [cache_entry, cache_miss] = CacheBase::getOrSet(key, load_func);
        if (cache_miss)
            ProfileEvents::increment(ProfileEvents::TextIndexHeaderCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexHeaderCacheHits);
        return std::move(cache_entry);
    }
};

/// Discriminators mixed into the cache key so the three cell kinds occupy disjoint key spaces.
/// Eager posting blocks keep the legacy two-component key `hash(index_id, offset)`; the lazy kinds
/// add a tag so a segment at byte offset X cannot collide with the eager block decoded from offset X.
enum class TextIndexPostingsCacheKind : UInt64
{
    Segment = 0x5345474D454E54ULL, /// "SEGMENT"
    Flat = 0x464C4154ULL,          /// "FLAT"
};

/// A single cell of TextIndexPostingsCache. It holds exactly one of:
///   - PostingList:           a decoded Roaring bitmap of one posting-list block (eager / materialize path);
///   - FlatPostingsPtr:       a flattened sorted array of analyzer-folded postings (lazy "prebuilt" cursor);
///   - PostingListSegmentPtr: a decoded segment (payload + per-block index) of a compressed posting list (lazy cursor).
/// The two lazy kinds are held by shared_ptr so an active cursor keeps its data alive even after the
/// cell is evicted from the (bounded) cache.
struct TextIndexPostingsCacheCell
{
    std::variant<PostingList, FlatPostingsPtr, PostingListSegmentPtr> value;
};

/// Estimate of the memory usage (bytes) of a posting cache cell
struct TextIndexPostingsWeightFunction
{
    size_t operator()(const TextIndexPostingsCacheCell & cell) const
    {
        if (const auto * postings = std::get_if<PostingList>(&cell.value))
            return postings->getSizeInBytes();
        if (const auto * flat = std::get_if<FlatPostingsPtr>(&cell.value))
            return *flat ? (*flat)->capacity() * sizeof(UInt32) : 0;
        if (const auto * segment = std::get_if<PostingListSegmentPtr>(&cell.value))
            return *segment ? (*segment)->bytesAllocated() : 0;
        return 0;
    }
};

class TextIndexPostingsCache : public CacheBase<UInt128, TextIndexPostingsCacheCell, UInt128TrivialHash, TextIndexPostingsWeightFunction>
{
public:
    TextIndexPostingsCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexPostingsCacheBytes, CurrentMetrics::TextIndexPostingsCacheCells, max_size_in_bytes, max_count, size_ratio)
    {}

    template <typename... Args>
    static UInt128 hash(Args... args)
    {
        SipHash hasher;
        (hasher.update(args),...);
        return hasher.get128();
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(UInt128 key, LoadFunc && load_func)
    {
        auto [cache_entry, cache_miss] = CacheBase::getOrSet(key, load_func);
        if (cache_miss)
            ProfileEvents::increment(ProfileEvents::TextIndexPostingsCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexPostingsCacheHits);
        return std::move(cache_entry);
    }
};

using TextIndexTokensCachePtr = std::shared_ptr<TextIndexTokensCache>;
using TextIndexHeaderCachePtr = std::shared_ptr<TextIndexHeaderCache>;
using TextIndexPostingsCachePtr = std::shared_ptr<TextIndexPostingsCache>;

class TokensCardinalitiesCache
{
public:
    explicit TokensCardinalitiesCache(std::vector<String> all_search_tokens_);

    void update(const TokenToPostingsInfosMap & token_infos, const absl::flat_hash_set<String> & missing_tokens, size_t total_rows);
    void sortTokens(std::vector<String> & tokens) const;

private:
    const std::vector<String> all_search_tokens;

    struct CardinalityAggregate
    {
        size_t cardinality = 0;
        size_t checked_rows = 0;

        bool operator==(const CardinalityAggregate & other) const = default;
    };

    mutable std::mutex mutex;
    using CardinalitiesMap = std::unordered_map<String, CardinalityAggregate>;
    CardinalitiesMap cardinalities TSA_GUARDED_BY(mutex);
};

}
