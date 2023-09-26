#include <gtest/gtest.h>

#include <Common/ProxyListConfigurationResolver.h>
#include <Poco/URI.h>

namespace
{
    auto proxy_server1 = Poco::URI("http://proxy_server1:3128");
    auto proxy_server2 = Poco::URI("http://proxy_server2:3128");
}

TEST(ProxyListConfigurationResolver, SimpleTest)
{
    bool use_connect_protocol = true;
    DB::ProxyListConfigurationResolver resolver({proxy_server1, proxy_server2}, use_connect_protocol);

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, DB::ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, use_connect_protocol);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, DB::ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, use_connect_protocol);
}

TEST(ProxyListConfigurationResolver, SimpleTestConnectProtocolOff)
{
    bool use_connect_protocol = false;
    DB::ProxyListConfigurationResolver resolver({proxy_server1, proxy_server2}, use_connect_protocol);

    auto configuration1 = resolver.resolve();
    auto configuration2 = resolver.resolve();

    ASSERT_EQ(configuration1.host, proxy_server1.getHost());
    ASSERT_EQ(configuration1.port, proxy_server1.getPort());
    ASSERT_EQ(configuration1.protocol, DB::ProxyConfiguration::protocolFromString(proxy_server1.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, use_connect_protocol);

    ASSERT_EQ(configuration2.host, proxy_server2.getHost());
    ASSERT_EQ(configuration2.port, proxy_server2.getPort());
    ASSERT_EQ(configuration2.protocol, DB::ProxyConfiguration::protocolFromString(proxy_server2.getScheme()));
    ASSERT_EQ(configuration1.use_connect_protocol, use_connect_protocol);
}
