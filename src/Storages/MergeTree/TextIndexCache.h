#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <vector>

#include <absl/container/flat_hash_map.h>

namespace ProfileEvents
{
    extern const Event TextIndexDictionaryBlockCacheHits;
    extern const Event TextIndexDictionaryBlockCacheMisses;
    extern const Event TextIndexHeaderCacheHits;
    extern const Event TextIndexHeaderCacheMisses;
    extern const Event TextIndexPostingsCacheHits;
    extern const Event TextIndexPostingsCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric TextIndexDictionaryBlockCacheBytes;
    extern const Metric TextIndexDictionaryBlockCacheCells;
    extern const Metric TextIndexHeaderCacheBytes;
    extern const Metric TextIndexHeaderCacheCells;
    extern const Metric TextIndexPostingsCacheBytes;
    extern const Metric TextIndexPostingsCacheCells;
}

namespace DB
{

class TextIndexDictionaryBlockCacheEntry
{
public:
    TextIndexDictionaryBlockCacheEntry() = default;

    explicit TextIndexDictionaryBlockCacheEntry(DictionaryBlock && dictionary_block)
    {
        const auto & tokens = assert_cast<const ColumnString &>(*dictionary_block.tokens);
        auto num_tokens = tokens.size();
        token_infos.reserve(num_tokens);
        for (size_t i = 0; i < num_tokens; ++i)
            token_infos.emplace(tokens.getDataAt(i), std::move(dictionary_block.token_infos[i]));
    }

    TokenPostingsInfo * getTokenInfo(std::string_view token)
    {
        if (auto it = token_infos.find(token); it != token_infos.end())
            return &it->second;
        return {};
    }

    size_t approximateMemoryUsage() const
    {
        static constexpr size_t embedded_posting_lists_size = 8; /// Assuming each embedded posting list has 8 entries.
        static constexpr size_t token_length = 5; /// Assuming each token is 5 chars long.

        const auto tokens_byte_size = token_infos.size() * sizeof(String) * token_length;
        /// We estimate 30% of postings lists are embedded
        const auto embedded_posting_lists = static_cast<size_t>(std::ceil(token_infos.size() * 0.3));
        const auto posting_lists_byte_size
            = (token_infos.size() * sizeof(TokenPostingsInfo)) + (embedded_posting_lists * embedded_posting_lists_size);
        return tokens_byte_size + posting_lists_byte_size;
    }

private:
    /// TokenPostingsInfo contains either an offset of a larger posting list or an array of rows directly in case
    /// of a posting list is small enough. In the latter case, the posting list will be cached as well.
    absl::flat_hash_map<String, TokenPostingsInfo> token_infos;
};

/// Estimate of the memory usage (bytes) of a dictionary block in cache
struct TextIndexDictionaryBlockWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t DICTIONARY_BLOCK_CACHE_OVERHEAD = sizeof(absl::flat_hash_map<String, TokenPostingsInfo>);

    size_t operator()(const TextIndexDictionaryBlockCacheEntry & dictionary_block) const
    {
        return dictionary_block.approximateMemoryUsage() + DICTIONARY_BLOCK_CACHE_OVERHEAD;
    }
};

class TextIndexDictionaryBlockCache : public CacheBase<UInt128, TextIndexDictionaryBlockCacheEntry, UInt128TrivialHash, TextIndexDictionaryBlockWeightFunction>
{
public:
    TextIndexDictionaryBlockCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexDictionaryBlockCacheBytes, CurrentMetrics::TextIndexDictionaryBlockCacheCells, max_size_in_bytes, max_count, size_ratio)
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
            ProfileEvents::increment(ProfileEvents::TextIndexDictionaryBlockCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexDictionaryBlockCacheHits);
        return std::move(cache_entry);
    }
};

/// Estimate of the memory usage (bytes) of a text index header in cache
struct TextIndexHeaderWeightFunction
{
    size_t operator()(const TextIndexHeader & header) const
    {
        return header.memoryUsageBytes();
    }
};

class TextIndexHeaderCache : public CacheBase<UInt128, TextIndexHeader, UInt128TrivialHash, TextIndexHeaderWeightFunction>
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

using TextIndexDictionaryBlockCacheEntryPtr = std::shared_ptr<TextIndexDictionaryBlockCacheEntry>;
using TextIndexDictionaryBlockCachePtr = std::shared_ptr<TextIndexDictionaryBlockCache>;

using TextIndexHeaderCachePtr = std::shared_ptr<TextIndexHeaderCache>;
using TextIndexPostingsCachePtr = std::shared_ptr<TextIndexPostingsCache>;
}
