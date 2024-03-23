#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/tests/gtest_helper_functions.h>
#include <Poco/URI.h>

namespace DB
{

namespace
{
    auto http_proxy_server = Poco::URI("http://proxy_server:3128");
    auto https_proxy_server = Poco::URI("https://proxy_server:3128");
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTP)
{
    EnvironmentProxySetter setter(http_proxy_server, {});

    EnvironmentProxyConfigurationResolver resolver(ProxyConfiguration::Protocol::HTTP);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::protocolFromString(http_proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPNoEnv)
{
    EnvironmentProxyConfigurationResolver resolver(ProxyConfiguration::Protocol::HTTP);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPs)
{
    EnvironmentProxySetter setter({}, https_proxy_server);

    EnvironmentProxyConfigurationResolver resolver(ProxyConfiguration::Protocol::HTTPS);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, https_proxy_server.getHost());
    ASSERT_EQ(configuration.port, https_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::protocolFromString(https_proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPsNoEnv)
{
    EnvironmentProxyConfigurationResolver resolver(ProxyConfiguration::Protocol::HTTPS);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPsOverHTTPTunnelingDisabled)
{
    // use http proxy for https, this would use connect protocol by default
    EnvironmentProxySetter setter({}, http_proxy_server);

    bool disable_tunneling_for_https_requests_over_http_proxy = true;

    EnvironmentProxyConfigurationResolver resolver(
        ProxyConfiguration::Protocol::HTTPS, disable_tunneling_for_https_requests_over_http_proxy);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, ProxyConfiguration::protocolFromString(http_proxy_server.getScheme()));
    ASSERT_EQ(configuration.tunneling, false);
}

}
