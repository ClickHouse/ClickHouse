#include "config.h"

#if USE_AHO_CORASICK

#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Functions/MultiSearchAhoCorasickCache.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event AhoCorasickCacheHit;
extern const Event AhoCorasickCacheMiss;
}

namespace DB
{
namespace
{

/// Every call returns an unused key, so the assertions on hit and miss counters hold even when the
/// process already populated the cache, for example under `--gtest_repeat`.
Array makeNeedles(const String & suffix)
{
    static std::atomic<size_t> counter{0};
    const String unique = suffix + "_" + std::to_string(counter.fetch_add(1));
    return {String("needle_") + unique, String("other_") + unique};
}

TEST(MultiSearchAhoCorasickCache, SharesConcurrentConstruction)
{
    setMultiSearchAutomatonCacheMaxSize(DEFAULT_MULTI_SEARCH_AUTOMATON_CACHE_MAX_SIZE);
    const auto needles = makeNeedles("concurrent");
    constexpr size_t num_threads = 8;
    std::barrier<> start(static_cast<ptrdiff_t>(num_threads));
    std::vector<std::shared_ptr<const AhoCorasickAutomaton>> automatons(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    const auto misses_before = ProfileEvents::global_counters[ProfileEvents::AhoCorasickCacheMiss];
    const auto hits_before = ProfileEvents::global_counters[ProfileEvents::AhoCorasickCacheHit];
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(
            [&, i]
            {
                start.arrive_and_wait();
                automatons[i] = getOrBuildAhoCorasickAutomaton(MultiSearchCaseMode::Sensitive, needles);
            });
    }
    for (auto & thread : threads)
        thread.join();

    for (const auto & automaton : automatons)
        EXPECT_EQ(automaton, automatons.front());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::AhoCorasickCacheMiss] - misses_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::AhoCorasickCacheHit] - hits_before, num_threads - 1);
}

TEST(MultiSearchAhoCorasickCache, DoesNotRetainOversizedAutomaton)
{
    const auto needles = makeNeedles("oversized");
    setMultiSearchAutomatonCacheMaxSize(1);
    auto first = getOrBuildAhoCorasickAutomaton(MultiSearchCaseMode::Sensitive, needles);
    auto second = getOrBuildAhoCorasickAutomaton(MultiSearchCaseMode::Sensitive, needles);

    EXPECT_GT(first->memory_bytes, 1);
    EXPECT_NE(first, second);
    setMultiSearchAutomatonCacheMaxSize(DEFAULT_MULTI_SEARCH_AUTOMATON_CACHE_MAX_SIZE);
}

}
}

#endif
