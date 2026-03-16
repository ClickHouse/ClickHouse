#include <Functions/AI/AIResultCache.h>
#include <Common/SipHash.h>
#include <IO/WriteHelpers.h>

namespace CurrentMetrics
{
    extern const Metric LLMCacheSizeInBytes;
    extern const Metric LLMCacheEntries;
}

namespace DB
{

static constexpr size_t DEFAULT_LLM_CACHE_MAX_SIZE = 1ULL << 30; // 1 GiB
static constexpr size_t DEFAULT_LLM_CACHE_MAX_ENTRIES = 1'000'000;

AIResultCache & AIResultCache::instance()
{
    static AIResultCache cache(DEFAULT_LLM_CACHE_MAX_SIZE, DEFAULT_LLM_CACHE_MAX_ENTRIES);
    return cache;
}

AIResultCache::AIResultCache(size_t max_size_in_bytes, size_t max_entries)
    : Base("LRU", CurrentMetrics::LLMCacheSizeInBytes, CurrentMetrics::LLMCacheEntries, max_size_in_bytes, max_entries, 0.5)
{
}

UInt128 AIResultCache::buildKey(
    const String & function_name,
    const String & model,
    float temperature,
    const std::vector<String> & arguments)
{
    SipHash hash;
    auto hash_string_with_length = [&](const String & s)
    {
        hash.update(s.size());
        hash.update(s);
    };
    hash_string_with_length(function_name);
    hash_string_with_length(model);
    auto temp_str = toString(temperature);
    hash_string_with_length(temp_str);
    for (const auto & arg : arguments)
        hash_string_with_length(arg);
    return hash.get128();
}

std::vector<AIResultCache::CacheEntryInfo> AIResultCache::dump() const
{
    std::vector<CacheEntryInfo> result;
    auto entries = Base::dump();
    result.reserve(entries.size());
    for (const auto & [key, entry] : entries)
    {
        result.push_back({
            .key_hash = key,
            .function_name = entry->function_name,
            .model = entry->model,
            .result_size_bytes = entry->result_size_bytes,
            .created_at = entry->created_at,
            .expires_at = entry->expires_at,
            .hit_count = entry->hit_count.load(std::memory_order_relaxed),
        });
    }
    return result;
}

void AIResultCache::clear()
{
    Base::clear();
}

}
