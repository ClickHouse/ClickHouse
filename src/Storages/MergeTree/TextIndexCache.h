#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCommon.h>

#include <absl/container/flat_hash_map.h>

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

template <typename Mapped, typename WeightFunction>
struct TextIndexCacheBase : public CacheBase<UInt128, Mapped, UInt128TrivialHash, WeightFunction>
{
    using Base = CacheBase<UInt128, Mapped, UInt128TrivialHash, WeightFunction>;
    using MappedPtr = typename Base::MappedPtr;

    TextIndexCacheBase(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio, CurrentMetrics::Metric bytes_metric, CurrentMetrics::Metric cells_metric)
        : Base(cache_policy, bytes_metric, cells_metric, max_size_in_bytes, max_count, size_ratio)
    {
    }
};

/// Estimate of the memory usage (bytes) of a dictionary block in cache
struct TextIndexTokensWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t TOKENS_CACHE_OVERHEAD = 64;

    size_t operator()(const TokenPostingsInfo & token_info) const
    {
        return token_info.bytesAllocated() + TOKENS_CACHE_OVERHEAD;
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
    size_t operator()(const DictionarySparseIndex & header) const
    {
        return header.memoryUsageBytes();
    }
};

class TextIndexHeaderCache : public CacheBase<UInt128, DictionarySparseIndex, UInt128TrivialHash, TextIndexHeaderWeightFunction>
{
public:
    TextIndexHeaderCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexHeaderCacheBytes, CurrentMetrics::TextIndexHeaderCacheCells, max_size_in_bytes, max_count, size_ratio)
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
            ProfileEvents::increment(ProfileEvents::TextIndexHeaderCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexHeaderCacheHits);
        return std::move(cache_entry);
    }
};

/// Estimate of the memory usage (bytes) of a text index posting list in cache
struct TextIndexPostingsWeightFunction
{
    size_t operator()(const PostingList & postings) const
    {
        return postings.getSizeInBytes();
    }
};

class TextIndexPostingsCache : public CacheBase<UInt128, PostingList, UInt128TrivialHash, TextIndexPostingsWeightFunction>
{
public:
    TextIndexPostingsCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexPostingsCacheBytes, CurrentMetrics::TextIndexPostingsCacheCells, max_size_in_bytes, max_count, size_ratio)
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
            ProfileEvents::increment(ProfileEvents::TextIndexPostingsCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexPostingsCacheHits);
        return std::move(cache_entry);
    }
};

using TextIndexTokensCachePtr = std::shared_ptr<TextIndexTokensCache>;
using TextIndexHeaderCachePtr = std::shared_ptr<TextIndexHeaderCache>;
using TextIndexPostingsCachePtr = std::shared_ptr<TextIndexPostingsCache>;

}
