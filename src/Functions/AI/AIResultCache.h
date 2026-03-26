#pragma once

#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Core/Types.h>
#include <chrono>
#include <mutex>


namespace DB
{

struct AICacheEntry
{
    String result;
    String function_name;
    String model;
    UInt64 result_size_bytes;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    std::atomic<UInt64> hit_count{0};
};

using AICacheEntryPtr = std::shared_ptr<AICacheEntry>;

struct AICacheWeightFunction
{
    size_t operator()(const AICacheEntry & entry) const
    {
        return sizeof(AICacheEntry) + entry.result.size();
    }
};

class AIResultCache : public CacheBase<UInt128, AICacheEntry, UInt128Hash, AICacheWeightFunction>
{
    using Base = CacheBase<UInt128, AICacheEntry, UInt128Hash, AICacheWeightFunction>;

public:
    using MappedPtr = typename Base::MappedPtr;

    static AIResultCache & instance();

    AIResultCache(size_t max_size_in_bytes, size_t max_count);

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
