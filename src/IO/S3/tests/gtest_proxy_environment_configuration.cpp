#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace
{
    auto proxy_server = Poco::URI("http://proxy_server:3128");
}

TEST(ProxyEnvironmentConfiguration, TestHTTP)
{
    setenv("http_proxy", proxy_server.toString().c_str(), 1);

    DB::EnvironmentProxyConfigurationResolver resolver;

    auto configuration = resolver.resolve(false).value();

    ASSERT_EQ(configuration.host, proxy_server.getHost());
    ASSERT_EQ(configuration.port, proxy_server.getPort());
    ASSERT_EQ(configuration.scheme, proxy_server.getScheme());
}

TEST(ProxyEnvironmentConfiguration, TestHTTPNoEnv)
{
    unsetenv("http_proxy");

    DB::EnvironmentProxyConfigurationResolver resolver;

    ASSERT_FALSE(resolver.resolve(false).has_value());
}

TEST(ProxyEnvironmentConfiguration, TestHTTPs)
{
    setenv("https_proxy", proxy_server.toString().c_str(), 1);

    DB::EnvironmentProxyConfigurationResolver resolver;

    auto configuration = resolver.resolve(true).value();

    ASSERT_EQ(configuration.host, proxy_server.getHost());
    ASSERT_EQ(configuration.port, proxy_server.getPort());
    ASSERT_EQ(configuration.scheme, proxy_server.getScheme());
}

TEST(ProxyEnvironmentConfiguration, TestHTTPsNoEnv)
{
    unsetenv("https_proxy");

    DB::EnvironmentProxyConfigurationResolver resolver;

    ASSERT_FALSE(resolver.resolve(true).has_value());
}
