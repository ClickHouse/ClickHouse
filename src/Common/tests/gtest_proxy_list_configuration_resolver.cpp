#include <gtest/gtest.h>

#include <Common/ProxyListConfigurationResolver.h>
#include <Poco/URI.h>

namespace DB
{

namespace
{
    auto proxy_server1 = Poco::URI("http://proxy_server1:3128");
    auto proxy_server2 = Poco::URI("http://proxy_server2:3128");

    std::string no_proxy_hosts = "localhost,,127.0.0.1,some_other_domain,,,, sub-domain.domain.com,";
}

TEST(ProxyListConfigurationResolver, SimpleTest)
{

    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTP,
        no_proxy_hosts);

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.no_proxy_hosts, no_proxy_hosts);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration2.no_proxy_hosts, no_proxy_hosts);
}

TEST(ProxyListConfigurationResolver, HTTPSRequestsOverHTTPProxyDefault)
{

    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTPS,
        "");

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.tunneling, true);
    ASSERT_EQ(configuration1.no_proxy_hosts, "");

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration2.no_proxy_hosts, "");
}

TEST(ProxyListConfigurationResolver, SimpleTestTunnelingDisabled)
{
    bool disable_tunneling_for_https_requests_over_http_proxy = true;
    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTPS,
        "",
        disable_tunneling_for_https_requests_over_http_proxy);

    auto configuration1 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.tunneling, false);
}

}
