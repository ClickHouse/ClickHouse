#include <gtest/gtest.h>

#include <Common/ProxyListConfigurationResolver.h>
#include <Poco/URI.h>

namespace DB
{

namespace
{
    auto proxy_server1 = Poco::URI("http://proxy_server1:3128");
    auto proxy_server2 = Poco::URI("http://proxy_server2:3128");
}

TEST(ProxyListConfigurationResolver, SimpleTest)
{

    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTP,
        ProxyConfigurationResolver::ConnectProtocolPolicy::DEFAULT
    );

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, false);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, false);
}

TEST(ProxyListConfigurationResolver, ConnectProtocolDefault)
{

    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTPS,
        ProxyConfigurationResolver::ConnectProtocolPolicy::DEFAULT
    );

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, true);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, true);
}

TEST(ProxyListConfigurationResolver, SimpleTestConnectProtocolOff)
{
    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTPS,
        ProxyConfigurationResolver::ConnectProtocolPolicy::FORCE_OFF
    );

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, false);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, false);
}

TEST(ProxyListConfigurationResolver, SimpleTestConnectProtocolOn)
{
    ProxyListConfigurationResolver resolver(
        {proxy_server1, proxy_server2},
        ProxyConfiguration::Protocol::HTTP,
        ProxyConfigurationResolver::ConnectProtocolPolicy::FORCE_ON
    );

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, true);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, true);
}

}
