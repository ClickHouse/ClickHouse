#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace
{
    auto proxy_server = Poco::URI("http://proxy_server:3128");
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTP)
{
    setenv("http_proxy", proxy_server.toString().c_str(), 1); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTP);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, proxy_server.getHost());
    ASSERT_EQ(configuration.port, proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::fromString(proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPNoEnv)
{
    unsetenv("http_proxy"); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTP);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPs)
{
    setenv("https_proxy", proxy_server.toString().c_str(), 1); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTPS);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, proxy_server.getHost());
    ASSERT_EQ(configuration.port, proxy_server.getPort());
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::fromString(proxy_server.getScheme()));
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPsNoEnv)
{
    unsetenv("https_proxy"); // NOLINT(concurrency-mt-unsafe)

    DB::EnvironmentProxyConfigurationResolver resolver(DB::ProxyConfiguration::Protocol::HTTPS);

    auto configuration = resolver.resolve();

    ASSERT_EQ(configuration.host, "");
    ASSERT_EQ(configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(configuration.port, 0u);
}
