#include <gtest/gtest.h>

#include <Common/DNSResolver.h>
#include <base/scope_guard.h>

#include <atomic>
#include <thread>

using namespace DB;

/// `SYSTEM DROP DNS CACHE` must leave the cache empty even when the periodic update runs at the same time.
TEST(DNSResolver, DropCacheDuringUpdate)
{
    constexpr size_t iterations = 200;

    auto & resolver = DNSResolver::instance();
    resolver.setDisableCacheFlag(false); /// `Common.ReverseDNS` leaves the cache disabled
    resolver.dropCache();

    /// An address literal is resolved without a DNS request.
    const String host = "127.0.0.1";

    /// A single updating thread, like `DNSCacheUpdater`.
    std::atomic<bool> stop = false;
    std::thread updater([&]
    {
        while (!stop)
        {
            resolver.updateCache(0);
            std::this_thread::yield();
        }
    });
    SCOPE_EXIT({
        stop = true;
        updater.join();
        resolver.dropCache();
    });

    for (size_t i = 0; i < iterations; ++i)
    {
        resolver.resolveHostAll(host);

        /// Wait until the updater refreshes the entry, so `host` is one of the hosts it updates.
        const auto added_at = resolver.cacheEntries().at(0).second.cached_at;
        while (resolver.cacheEntries().at(0).second.cached_at == added_at)
            std::this_thread::yield();

        resolver.dropCache();
        ASSERT_TRUE(resolver.cacheEntries().empty()) << "The cache is not empty after dropCache(), iteration " << i;
    }
}
