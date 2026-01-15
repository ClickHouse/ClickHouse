#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCommon.h>

#include <vector>

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
        auto [cache_entry, cache_miss] = Base::getOrSet(key, load_func);
        if (cache_miss)
            ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheHits);
        return std::move(cache_entry);
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

class TextIndexTokensCache : public TextIndexCacheBase<TokenPostingsInfo, TextIndexTokensWeightFunction>
{
public:
    TextIndexTokensCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : TextIndexCacheBase(cache_policy, max_size_in_bytes, max_count, size_ratio, CurrentMetrics::TextIndexTokensCacheBytes, CurrentMetrics::TextIndexTokensCacheCells)
    {
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

class TextIndexHeaderCache : public TextIndexCacheBase<DictionarySparseIndex, TextIndexHeaderWeightFunction>
{
public:
    TextIndexHeaderCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : TextIndexCacheBase(cache_policy, max_size_in_bytes, max_count, size_ratio, CurrentMetrics::TextIndexHeaderCacheBytes, CurrentMetrics::TextIndexHeaderCacheCells)
    {
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

class TextIndexPostingsCache : public TextIndexCacheBase<PostingList, TextIndexPostingsWeightFunction>
{
public:
    TextIndexPostingsCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : TextIndexCacheBase(cache_policy, max_size_in_bytes, max_count, size_ratio, CurrentMetrics::TextIndexPostingsCacheBytes, CurrentMetrics::TextIndexPostingsCacheCells)
    {
    }
};

using TextIndexTokensCachePtr = std::shared_ptr<TextIndexTokensCache>;
using TextIndexHeaderCachePtr = std::shared_ptr<TextIndexHeaderCache>;
using TextIndexPostingsCachePtr = std::shared_ptr<TextIndexPostingsCache>;

}
