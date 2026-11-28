#include <gtest/gtest.h>

#include <Common/RemoteProxyConfigurationResolver.h>
#include <Poco/URI.h>
#include <IO/ConnectionTimeouts.h>
#include <base/sleep.h>

#include "config.h"

#if USE_SILK
#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>
#include <base/scope_guard.h>

#include <Poco/Event.h>

#include <silk/fibers/event.h>
#include <silk/fibers/future.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>
#endif

namespace
{

struct RemoteProxyHostFetcherMock : public DB::RemoteProxyHostFetcher
{
    explicit RemoteProxyHostFetcherMock(const std::string & return_mock_) : return_mock(return_mock_) {}

    std::string fetch(const Poco::URI &, const DB::ConnectionTimeouts &) override
    {
        fetch_count++;
        return return_mock;
    }

    std::string return_mock;
    std::size_t fetch_count {0};
};

}


namespace DB
{

TEST(RemoteProxyConfigurationResolver, HTTPOverHTTP)
{
    const char * proxy_server_mock = "proxy1";
    auto remote_server_configuration = RemoteProxyConfigurationResolver::RemoteServerConfiguration
    {
        Poco::URI("not_important"),
        "http",
        80,
        std::chrono::seconds {10}
    };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTP,
        "",
        std::make_shared<RemoteProxyHostFetcherMock>(proxy_server_mock)
    );

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, proxy_server_mock);
    ASSERT_EQ(configuration.port, 80);
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.original_request_protocol, ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.tunneling, false);
}

TEST(RemoteProxyConfigurationResolver, HTTPSOverHTTPS)
{
    const char * proxy_server_mock = "proxy1";
    auto remote_server_configuration = RemoteProxyConfigurationResolver::RemoteServerConfiguration
    {
        Poco::URI("not_important"),
        "https",
        443,
        std::chrono::seconds {10}
    };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTPS,
        "",
        std::make_shared<RemoteProxyHostFetcherMock>(proxy_server_mock)
    );

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, proxy_server_mock);
    ASSERT_EQ(configuration.port, 443);
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::Protocol::HTTPS);
    ASSERT_EQ(configuration.original_request_protocol, ProxyConfiguration::Protocol::HTTPS);
    // tunneling should not be used, https over https.
    ASSERT_EQ(configuration.tunneling, false);
}

TEST(RemoteProxyConfigurationResolver, HTTPSOverHTTP)
{
    const char * proxy_server_mock = "proxy1";
    auto remote_server_configuration = RemoteProxyConfigurationResolver::RemoteServerConfiguration
    {
        Poco::URI("not_important"),
        "http",
        80,
        std::chrono::seconds {10}
    };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTPS,
        "",
        std::make_shared<RemoteProxyHostFetcherMock>(proxy_server_mock)
    );

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, proxy_server_mock);
    ASSERT_EQ(configuration.port, 80);
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.original_request_protocol, ProxyConfiguration::Protocol::HTTPS);
    // tunneling should be used, https over http.
    ASSERT_EQ(configuration.tunneling, true);
}

TEST(RemoteProxyConfigurationResolver, HTTPSOverHTTPNoTunneling)
{
    const char * proxy_server_mock = "proxy1";
    auto remote_server_configuration = RemoteProxyConfigurationResolver::RemoteServerConfiguration
        {
            Poco::URI("not_important"),
            "http",
            80,
            std::chrono::seconds {10}
        };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTPS,
        "",
        std::make_shared<RemoteProxyHostFetcherMock>(proxy_server_mock),
        true /* disable_tunneling_for_https_requests_over_http_proxy_ */
    );

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, proxy_server_mock);
    ASSERT_EQ(configuration.port, 80);
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.original_request_protocol, ProxyConfiguration::Protocol::HTTPS);
    // tunneling should be used, https over http.
    ASSERT_EQ(configuration.tunneling, false);
}

TEST(RemoteProxyConfigurationResolver, SimpleCacheTest)
{
    const char * proxy_server_mock = "proxy1";
    auto cache_ttl = 5u;
    auto remote_server_configuration = RemoteProxyConfigurationResolver::RemoteServerConfiguration
    {
        Poco::URI("not_important"),
        "http",
        80,
        std::chrono::seconds {cache_ttl}
    };

    auto fetcher_mock = std::make_shared<RemoteProxyHostFetcherMock>(proxy_server_mock);

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTP,
        "",
        fetcher_mock
    );

    resolver.resolve();
    resolver.resolve();
    resolver.resolve();

    ASSERT_EQ(fetcher_mock->fetch_count, 1u);

    sleepForSeconds(cache_ttl * 2);

    resolver.resolve();

    ASSERT_EQ(fetcher_mock->fetch_count, 2);
}

#if USE_SILK

namespace
{

/// Suspends the fiber that fetches, with the cache lock held, until `release` is set.
struct SuspendingFetcher : public RemoteProxyHostFetcher
{
    std::string fetch(const Poco::URI &, const ConnectionTimeouts &) override
    {
        ++fetch_count;
        release.wait();
        return "proxy1";
    }

    std::atomic<size_t> fetch_count = 0;
    silk::FiberEvent release;
};

}

/// The fetch can suspend a fiber, for example in a DNS lookup, with the cache lock held. There are more fibers than the
/// threads that may run them, so all of them start only if the fibers that wait for the lock suspend instead of blocking
/// their threads. All of them get the proxy of a single fetch.
TEST(RemoteProxyConfigurationResolver, ConcurrentFibersWaitForOneFetch)
{
    initializeFiberSchedulerForTests();

    /// Created after the scheduler, so that its `Silk::FiberMutex` is a fiber mutex.
    auto fetcher = std::make_shared<SuspendingFetcher>();
    RemoteProxyConfigurationResolver resolver(
        {Poco::URI("not_important"), "http", 80, std::chrono::seconds{10}},
        ProxyConfiguration::Protocol::HTTP,
        "",
        fetcher);

    const size_t fiber_count = 4 * std::max(1U, std::thread::hardware_concurrency()) + 1;
    std::atomic<size_t> started = 0;
    Poco::Event all_started(/* autoReset = */ false);
    std::vector<std::string> hosts(fiber_count);
    std::vector<silk::FiberFuture> futures(fiber_count);

    size_t spawned = 0;
    {
        /// The started fibers use the locals above, so the fetch is released and they finish before the results are
        /// checked, also when a spawn throws or the fibers do not start in time.
        SCOPE_EXIT({
            fetcher->release.set();
            for (size_t i = 0; i < spawned; ++i)
                EXPECT_EQ(futures[i].wait(), 0);
        });

        while (spawned < fiber_count && Silk::spawn([&, i = spawned]
            {
                if (++started == fiber_count)
                    all_started.set();
                hosts[i] = resolver.resolve().host;
                return 0;
            }, futures[spawned]) == 0)
            ++spawned;
        ASSERT_EQ(spawned, fiber_count);

        /// Checked before the release: fibers that block their threads would never let all of them start.
        EXPECT_TRUE(all_started.tryWait(60'000)) << "The fibers did not start within 60 seconds";
    }

    EXPECT_EQ(fetcher->fetch_count, 1);
    for (size_t i = 0; i < fiber_count; ++i)
        EXPECT_EQ(hosts[i], "proxy1") << "fiber " << i;
}

#endif

}
