#include <Functions/MultiSearchAhoCorasickCache.h>

#if USE_AHO_CORASICK

#include <vector>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

namespace CurrentMetrics
{
    extern const Metric MultiSearchAutomatonCacheBytes;
    extern const Metric MultiSearchAutomatonCacheCount;
}

namespace ProfileEvents
{
    extern const Event AhoCorasickCacheHit;
    extern const Event AhoCorasickCacheMiss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

MultiSearchAhoCorasickCacheFactory & MultiSearchAhoCorasickCacheFactory::instance()
{
    static MultiSearchAhoCorasickCacheFactory factory;
    return factory;
}

void MultiSearchAhoCorasickCacheFactory::init(size_t cache_size_in_bytes, size_t cache_size_in_elements)
{
    if (cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiSearchAhoCorasickCache was already initialized");

    cache = std::make_unique<MultiSearchAhoCorasickCache>(
        CurrentMetrics::MultiSearchAutomatonCacheBytes,
        CurrentMetrics::MultiSearchAutomatonCacheCount,
        cache_size_in_bytes,
        cache_size_in_elements);
}

MultiSearchAhoCorasickCache * MultiSearchAhoCorasickCacheFactory::tryGetCache()
{
    return cache.get();
}

AhoCorasickAutomaton::~AhoCorasickAutomaton()
{
    /// Freeing the Rust handle routes through ClickHouse's allocator, which decrements the current
    /// thread's memory tracker. The matching allocation in buildAutomaton runs under a
    /// MemoryTrackerBlockerInThread, so block here too to keep the tracker balanced: the automaton
    /// outlives the query that built it and may be freed on an unrelated thread during eviction.
    MemoryTrackerBlockerInThread memory_blocker;
    if (handle)
        aho_corasick_free(handle);
}

namespace
{

/// Approximate per-entry bookkeeping overhead (shared_ptr control block, hash-map node, key).
constexpr size_t AUTOMATON_ENTRY_OVERHEAD = 256;

UInt128 computeKey(uint8_t case_mode, const Array & needles)
{
    SipHash hash;
    hash.update(case_mode);
    for (const auto & needle : needles)
    {
        const String & s = needle.safeGet<String>();
        hash.update(s.size());
        hash.update(s.data(), s.size());
    }
    return hash.get128();
}

std::shared_ptr<AhoCorasickAutomaton> buildAutomaton(uint8_t case_mode, const Array & needles)
{
    /// The automaton stays in the server-global cache and outlives the current query, so do not
    /// charge its memory to the query's memory tracker.
    MemoryTrackerBlockerInThread memory_blocker;

    std::vector<const uint8_t *> pattern_ptrs;
    std::vector<uint64_t> pattern_sizes;
    pattern_ptrs.reserve(needles.size());
    pattern_sizes.reserve(needles.size());

    for (const auto & needle : needles)
    {
        const String & s = needle.safeGet<String>();
        pattern_ptrs.push_back(reinterpret_cast<const uint8_t *>(s.data()));
        pattern_sizes.push_back(s.size());
    }

    AhoCorasickHandle * handle = aho_corasick_create(
        pattern_ptrs.data(),
        pattern_sizes.data(),
        static_cast<uint64_t>(pattern_ptrs.size()),
        case_mode);

    if (!handle)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Failed to build Aho-Corasick automaton for {} patterns (too many states or out of memory)",
            needles.size());

    const size_t memory_bytes = aho_corasick_heap_bytes(handle) + AUTOMATON_ENTRY_OVERHEAD;
    return std::make_shared<AhoCorasickAutomaton>(handle, memory_bytes);
}

}

std::shared_ptr<const AhoCorasickAutomaton> getOrBuildAhoCorasickAutomaton(uint8_t case_mode, const Array & needles)
{
    auto * cache = MultiSearchAhoCorasickCacheFactory::instance().tryGetCache();
    if (!cache)
        /// Cache not initialized (e.g. tooling contexts without server startup): build uncached.
        return buildAutomaton(case_mode, needles);

    const UInt128 key = computeKey(case_mode, needles);
    auto [automaton, produced] = cache->getOrSet(key, [&] { return buildAutomaton(case_mode, needles); });

    ProfileEvents::increment(produced ? ProfileEvents::AhoCorasickCacheMiss : ProfileEvents::AhoCorasickCacheHit);
    return automaton;
}

}

#endif
