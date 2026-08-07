#include <gtest/gtest.h>

#include <Common/RemoteProxyConfigurationResolver.h>
#include <Poco/URI.h>
#include <IO/ConnectionTimeouts.h>
#include <base/sleep.h>

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

}
