#include <gtest/gtest.h>

#include <Common/RemoteProxyConfigurationResolver.h>
#include <Poco/URI.h>
#include <IO/ConnectionTimeouts.h>

namespace
{

struct RemoteProxyHostFetcherMock : public DB::RemoteProxyHostFetcher
{
    explicit RemoteProxyHostFetcherMock(const std::string & return_mock_) : return_mock(return_mock_) {}

    std::string fetch(const Poco::URI &, const DB::ConnectionTimeouts &) const override
    {
        return return_mock;
    }

    std::string return_mock;
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
        10
    };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTP,
        std::make_unique<RemoteProxyHostFetcherMock>(proxy_server_mock)
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
        10
    };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTPS,
        std::make_unique<RemoteProxyHostFetcherMock>(proxy_server_mock)
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
        10
    };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTPS,
        std::make_unique<RemoteProxyHostFetcherMock>(proxy_server_mock)
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
            10
        };

    RemoteProxyConfigurationResolver resolver(
        remote_server_configuration,
        ProxyConfiguration::Protocol::HTTPS,
        std::make_unique<RemoteProxyHostFetcherMock>(proxy_server_mock),
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

}
