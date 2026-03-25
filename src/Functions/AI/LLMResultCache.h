#pragma once

#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Core/Types.h>
#include <chrono>
#include <mutex>


namespace DB
{

struct LLMCacheEntry
{
    String result;
    String function_name;
    String model;
    UInt64 result_size_bytes;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    std::atomic<UInt64> hit_count{0};
};

using LLMCacheEntryPtr = std::shared_ptr<LLMCacheEntry>;

struct LLMCacheWeightFunction
{
    size_t operator()(const LLMCacheEntry & entry) const
    {
        return sizeof(LLMCacheEntry) + entry.result.size();
    }
};

class LLMResultCache : public CacheBase<UInt128, LLMCacheEntry, UInt128Hash, LLMCacheWeightFunction>
{
    using Base = CacheBase<UInt128, LLMCacheEntry, UInt128Hash, LLMCacheWeightFunction>;

public:
    using MappedPtr = typename Base::MappedPtr;

    static LLMResultCache & instance();

    LLMResultCache(size_t max_size_in_bytes, size_t max_count);

    static UInt128 buildKey(
        const String & function_name,
        const String & model,
        float temperature,
        const std::vector<String> & arguments);

    struct CacheEntryInfo
    {
        UInt128 key_hash;
        String function_name;
        String model;
        UInt64 result_size_bytes;
        std::chrono::system_clock::time_point created_at;
        std::chrono::system_clock::time_point expires_at;
        UInt64 hit_count;
    };

    std::vector<CacheEntryInfo> getEntries() const;

    void reset();
};

}
