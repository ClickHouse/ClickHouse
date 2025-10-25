#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <vector>

#include <absl/container/flat_hash_map.h>

namespace ProfileEvents
{
    extern const Event TextIndexDictionaryBlockCacheHits;
    extern const Event TextIndexDictionaryBlockCacheMisses;
    extern const Event TextIndexPostingListCacheHits;
    extern const Event TextIndexPostingListCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric TextIndexDictionaryBlockCacheBytes;
    extern const Metric TextIndexDictionaryBlockCacheCells;
    extern const Metric TextIndexPostingListCacheBytes;
    extern const Metric TextIndexPostingListCacheCells;
}

namespace DB
{

class TextIndexDictionaryBlockCacheEntry
{
public:
    TextIndexDictionaryBlockCacheEntry() = default;
    TextIndexDictionaryBlockCacheEntry(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_)
    {
        const auto & tokens = assert_cast<const ColumnString &>(*tokens_);
        auto num_tokens = tokens.size();
        token_infos.reserve(num_tokens);
        for (size_t i = 0; i < num_tokens; ++i)
            token_infos.emplace(tokens.getDataAt(i), std::move(token_infos_[i]));
    }

    TokenPostingsInfo * getTokenInfo(const StringRef & token)
    {
        if (auto it = token_infos.find(token.toString()); it != token_infos.end())
            return &it->second;
        return {};
    }

private:
    absl::flat_hash_map<String, TokenPostingsInfo> token_infos;
};

class TextIndexDictionaryBlockCache : public CacheBase<UInt128, TextIndexDictionaryBlockCacheEntry, UInt128TrivialHash>
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

class TextIndexPostingListCache : public CacheBase<UInt128, PostingList, UInt128TrivialHash>
{
public:
    TextIndexPostingListCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : CacheBase(cache_policy, CurrentMetrics::TextIndexPostingListCacheBytes, CurrentMetrics::TextIndexPostingListCacheCells, max_size_in_bytes, max_count, size_ratio)
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
            ProfileEvents::increment(ProfileEvents::TextIndexPostingListCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexPostingListCacheHits);
        return std::move(cache_entry);
    }
};

using TextIndexDictionaryBlockCacheEntryPtr = std::shared_ptr<TextIndexDictionaryBlockCacheEntry>;
using TextIndexDictionaryBlockCachePtr = std::shared_ptr<TextIndexDictionaryBlockCache>;

using TextIndexPostingListCachePtr = std::shared_ptr<TextIndexPostingListCache>;
}
