#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <vector>

#include <absl/container/flat_hash_map.h>

namespace ProfileEvents
{
    extern const Event TextIndexDictionaryBlockCacheHits;
    extern const Event TextIndexDictionaryBlockCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric TextIndexDictionaryBlockCacheBytes;
    extern const Metric TextIndexDictionaryBlockCacheCells;
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

using TextIndexDictionaryBlockCacheEntryPtr = std::shared_ptr<TextIndexDictionaryBlockCacheEntry>;

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
    TextIndexDictionaryBlockCacheEntryPtr getOrSet(UInt128 key, LoadFunc && load_func)
    {
        auto [cache_entry, inserted] = CacheBase::getOrSet(key, load_func);
        if (inserted)
            ProfileEvents::increment(ProfileEvents::TextIndexDictionaryBlockCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::TextIndexDictionaryBlockCacheHits);
        return std::move(cache_entry);
    }
};

using TextIndexDictionaryBlockCachePtr = std::shared_ptr<TextIndexDictionaryBlockCache>;

}
