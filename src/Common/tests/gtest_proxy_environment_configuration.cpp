#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/tests/gtest_helper_functions.h>
#include <Poco/URI.h>

namespace
{
    auto http_proxy_server = Poco::URI("http://proxy_server:3128");
    auto https_proxy_server = Poco::URI("https://proxy_server:3128");
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTP)
{
    EnvironmentProxySetter setter(http_proxy_server, {});

    bool use_connect_protocol = true;
    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTP, use_connect_protocol);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_proxy_server.getScheme()));
    ASSERT_EQ(configuration.use_connect_protocol, use_connect_protocol);
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPConnectProtocolOff)
{
    EnvironmentProxySetter setter(http_proxy_server, {});

    bool use_connect_protocol = false;
    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTP, use_connect_protocol);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_proxy_server.getScheme()));
    ASSERT_EQ(configuration.use_connect_protocol, use_connect_protocol);
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPNoEnv)
{
    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTP, true);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPs)
{
    EnvironmentProxySetter setter({}, https_proxy_server);

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTPS, true);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, https_proxy_server.getHost());
    ASSERT_EQ(configuration.port, https_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPsNoEnv)
{
    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTPS, true);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}

TEST(EnvironmentProxyConfigurationResolver, TestANYHTTP)
{
    EnvironmentProxySetter setter(http_proxy_server, {});

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::ANY, true);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestANYHTTPS)
{
    EnvironmentProxySetter setter({}, https_proxy_server);

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::ANY, true);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, https_proxy_server.getHost());
    ASSERT_EQ(configuration.port, https_proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestANYNoEnv)
{
    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::ANY, true);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}
