#include <gtest/gtest.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/tests/gtest_global_context.h>

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

namespace
{
    Poco::URI http_proxy_server = Poco::URI("http://http_environment_proxy:3128");
    Poco::URI https_proxy_server = Poco::URI("http://https_environment_proxy:3128");
}

TEST(ProxyConfigurationResolverProviderTests, EnvironmentResolverShouldBeUsedIfNoSettings)
{
    setenv("http_proxy", http_proxy_server.toString().c_str(), 1); // NOLINT(concurrency-mt-unsafe)
    setenv("https_proxy", https_proxy_server.toString().c_str(), 1); // NOLINT(concurrency-mt-unsafe)

    auto http_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();
    auto https_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    ASSERT_EQ(http_configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(http_configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(http_configuration.protocol, DB::ProxyConfiguration::fromString(http_proxy_server.getScheme()));

    ASSERT_EQ(https_configuration.host, https_proxy_server.getHost());
    ASSERT_EQ(https_configuration.port, https_proxy_server.getPort());
    ASSERT_EQ(https_configuration.protocol, DB::ProxyConfiguration::fromString(https_proxy_server.getScheme()));

    unsetenv("http_proxy"); // NOLINT(concurrency-mt-unsafe)
    unsetenv("https_proxy"); // NOLINT(concurrency-mt-unsafe)
}

TEST(ProxyConfigurationResolverProviderTests, LIST_HTTP_ONLY)
{
    const auto & context_holder = getContext();
    auto context = context_holder.context;

    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.http", "");
    config->setString("proxy.http.uri", http_proxy_server.toString());
    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::fromString(http_proxy_server.getScheme()));

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    // No https configuration since it's not set
    ASSERT_EQ(https_proxy_configuration.host, "");
    ASSERT_EQ(https_proxy_configuration.port, 0);
}

TEST(ProxyConfigurationResolverProviderTests, LIST_HTTPS_ONLY)
{
    const auto & context_holder = getContext();
    auto context = context_holder.context;

    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.https", "");
    config->setString("proxy.https.uri", "http://list_proxy1:3128");
    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, "");
    ASSERT_EQ(http_proxy_configuration.port, 0);

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    ASSERT_EQ(https_proxy_configuration.host, "list_proxy1");

    // still HTTP because the proxy host is not HTTPS
    ASSERT_EQ(https_proxy_configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(https_proxy_configuration.port, 3128);
}

TEST(ProxyConfigurationResolverProviderTests, LIST_HTTP_BOTH)
{
    const auto & context_holder = getContext();
    auto context = context_holder.context;

    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.http", "");
    config->setString("proxy.http.uri", "http://http_proxy:3128");

    config->setString("proxy", "");
    config->setString("proxy.https", "");
    config->setString("proxy.https.uri", "https://https_proxy:3128");

    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, "http_proxy");
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::Protocol::HTTP);
    ASSERT_EQ(http_proxy_configuration.port, 3128);

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    ASSERT_EQ(https_proxy_configuration.host, "https_proxy");

    // still HTTP because the proxy host is not HTTPS
    ASSERT_EQ(https_proxy_configuration.protocol, DB::ProxyConfiguration::Protocol::HTTPS);
    ASSERT_EQ(https_proxy_configuration.port, 3128);
}

// remote resolver is tricky to be tested in unit tests
