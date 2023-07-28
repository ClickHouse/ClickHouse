#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace
{
    auto proxy_server = Poco::URI("http://proxy_server:3128");
}

TEST(ProxyEnvironmentConfiguration, TestHTTP)
{
    setenv("http_proxy", proxy_server.toString().c_str(), 1); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver;

    auto configuration = resolver.resolve(DB::ProxyConfiguration::Protocol::HTTP);

    ASSERT_EQ(configuration.host, proxy_server.getHost());
    ASSERT_EQ(configuration.port, proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::fromString(proxy_server.getScheme()));
}

TEST(ProxyEnvironmentConfiguration, TestHTTPNoEnv)
{
    unsetenv("http_proxy"); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver;

    auto configuration = resolver.resolve(DB::ProxyConfiguration::Protocol::HTTP);

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}

TEST(ProxyEnvironmentConfiguration, TestHTTPs)
{
    setenv("https_proxy", proxy_server.toString().c_str(), 1); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver;

    auto configuration = resolver.resolve(DB::ProxyConfiguration::Protocol::HTTPS);

    ASSERT_EQ(configuration.host, proxy_server.getHost());
    ASSERT_EQ(configuration.port, proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::fromString(proxy_server.getScheme()));
}

TEST(ProxyEnvironmentConfiguration, TestHTTPsNoEnv)
{
    unsetenv("https_proxy"); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver;

    auto configuration = resolver.resolve(DB::ProxyConfiguration::Protocol::HTTPS);

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}
