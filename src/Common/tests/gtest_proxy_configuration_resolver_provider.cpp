#include <gtest/gtest.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_helper_functions.h>

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

class ProxyConfigurationResolverProviderTests : public ::testing::Test
{
protected:

    static void SetUpTestSuite() {
        context = getContext().context;
    }

    static void TearDownTestSuite() {
        context->setConfig(Poco::AutoPtr(new Poco::Util::MapConfiguration()));
    }

    static DB::ContextMutablePtr context;
};

DB::ContextMutablePtr ProxyConfigurationResolverProviderTests::context;

Poco::URI http_env_proxy_server = Poco::URI("http://http_environment_proxy:3128");
Poco::URI https_env_proxy_server = Poco::URI("http://https_environment_proxy:3128");

Poco::URI http_list_proxy_server = Poco::URI("http://http_list_proxy:3128");
Poco::URI https_list_proxy_server = Poco::URI("http://https_list_proxy:3128");

TEST_F(ProxyConfigurationResolverProviderTests, EnvironmentResolverShouldBeUsedIfNoSettings)
{
    EnvironmentProxySetter setter(http_env_proxy_server, https_env_proxy_server);

    auto http_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();
    auto https_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    ASSERT_EQ(http_configuration.host, http_env_proxy_server.getHost());
    ASSERT_EQ(http_configuration.port, http_env_proxy_server.getPort());
    ASSERT_EQ(http_configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_env_proxy_server.getScheme()));

    ASSERT_EQ(https_configuration.host, https_env_proxy_server.getHost());
    ASSERT_EQ(https_configuration.port, https_env_proxy_server.getPort());
    ASSERT_EQ(https_configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_env_proxy_server.getScheme()));
}

TEST_F(ProxyConfigurationResolverProviderTests, ListHTTPOnly)
{
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.http", "");
    config->setString("proxy.http.uri", http_list_proxy_server.toString());
    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, http_list_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.port, http_list_proxy_server.getPort());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_list_proxy_server.getScheme()));

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    // No https configuration since it's not set
    ASSERT_EQ(https_proxy_configuration.host, "");
    ASSERT_EQ(https_proxy_configuration.port, 0);
}

TEST_F(ProxyConfigurationResolverProviderTests, ListHTTPSOnly)
{
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.https", "");
    config->setString("proxy.https.uri", https_list_proxy_server.toString());
    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, "");
    ASSERT_EQ(http_proxy_configuration.port, 0);

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    ASSERT_EQ(https_proxy_configuration.host, https_list_proxy_server.getHost());

    // still HTTP because the proxy host is not HTTPS
    ASSERT_EQ(https_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_list_proxy_server.getScheme()));
    ASSERT_EQ(https_proxy_configuration.port, https_list_proxy_server.getPort());
}

TEST_F(ProxyConfigurationResolverProviderTests, ListBoth)
{
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.http", "");
    config->setString("proxy.http.uri", http_list_proxy_server.toString());

    config->setString("proxy", "");
    config->setString("proxy.https", "");
    config->setString("proxy.https.uri", https_list_proxy_server.toString());

    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, http_list_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_list_proxy_server.getScheme()));
    ASSERT_EQ(http_proxy_configuration.port, http_list_proxy_server.getPort());

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS)->resolve();

    ASSERT_EQ(https_proxy_configuration.host, https_list_proxy_server.getHost());

    // still HTTP because the proxy host is not HTTPS
    ASSERT_EQ(https_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_list_proxy_server.getScheme()));
    ASSERT_EQ(https_proxy_configuration.port, https_list_proxy_server.getPort());
}

// remote resolver is tricky to be tested in unit tests
