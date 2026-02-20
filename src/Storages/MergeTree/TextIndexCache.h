#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <vector>

namespace ProfileEvents
{
    extern const Event TextIndexHeaderCacheHits;
    extern const Event TextIndexHeaderCacheMisses;
    extern const Event TextIndexPostingsCacheHits;
    extern const Event TextIndexPostingsCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric TextIndexHeaderCacheBytes;
    extern const Metric TextIndexHeaderCacheCells;
    extern const Metric TextIndexPostingsCacheBytes;
    extern const Metric TextIndexPostingsCacheCells;
}

namespace DB
{

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
    {}

    template <typename... ARGS>
    static UInt128 hash(ARGS... args)
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
    {}

    template <typename... ARGS>
    static UInt128 hash(ARGS... args)
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

using TextIndexHeaderCachePtr = std::shared_ptr<TextIndexHeaderCache>;
using TextIndexPostingsCachePtr = std::shared_ptr<TextIndexPostingsCache>;

}
