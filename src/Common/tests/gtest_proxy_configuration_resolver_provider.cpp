#include <gtest/gtest.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_helper_functions.h>

#include <Poco/Util/MapConfiguration.h>

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
    const auto & config = getContext().context->getConfigRef();

    auto http_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP, config)->resolve();
    auto https_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS, config)->resolve();

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

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP, *config)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, http_list_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.port, http_list_proxy_server.getPort());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_list_proxy_server.getScheme()));

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS, *config)->resolve();

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

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP, *config)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, "");
    ASSERT_EQ(http_proxy_configuration.port, 0);

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS, *config)->resolve();

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

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP, *config)->resolve();

    ASSERT_EQ(http_proxy_configuration.host, http_list_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_list_proxy_server.getScheme()));
    ASSERT_EQ(http_proxy_configuration.port, http_list_proxy_server.getPort());

    auto https_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS, *config)->resolve();

    ASSERT_EQ(https_proxy_configuration.host, https_list_proxy_server.getHost());

    // still HTTP because the proxy host is not HTTPS
    ASSERT_EQ(https_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_list_proxy_server.getScheme()));
    ASSERT_EQ(https_proxy_configuration.port, https_list_proxy_server.getPort());
}

TEST_F(ProxyConfigurationResolverProviderTests, RemoteResolverIsBasedOnProtocolConfigurationHTTP)
{
    /*
     * Since there is no way to call `ProxyConfigurationResolver::resolve` on remote resolver,
     * it is hard to verify the remote resolver was actually picked. One hackish way to assert
     * the remote resolver was OR was not picked based on the configuration, is to use the
     * environment resolver. Since the environment resolver is always returned as a fallback,
     * we can assert the remote resolver was not picked if `ProxyConfigurationResolver::resolve`
     * succeeds and returns an environment proxy configuration.
     * */
    EnvironmentProxySetter setter(http_env_proxy_server, https_env_proxy_server);

    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.https", "");
    config->setString("proxy.https.resolver", "");
    config->setString("proxy.https.resolver.endpoint", "http://resolver:8080/hostname");

    // even tho proxy protocol / scheme is http, it should not be picked (prior to this PR, it would be picked)
    config->setString("proxy.https.resolver.proxy_scheme", "http");
    config->setString("proxy.https.resolver.proxy_port", "80");
    config->setString("proxy.https.resolver.proxy_cache_time", "10");

    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTP, *config)->resolve();

    /*
     * Asserts env proxy is used and not the remote resolver. If the remote resolver is picked, it is an error because
     * there is no `http` specification for remote resolver
     * */
    ASSERT_EQ(http_proxy_configuration.host, http_env_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.port, http_env_proxy_server.getPort());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(http_env_proxy_server.getScheme()));
}

TEST_F(ProxyConfigurationResolverProviderTests, RemoteResolverIsBasedOnProtocolConfigurationHTTPS)
{
    /*
     * Since there is no way to call `ProxyConfigurationResolver::resolve` on remote resolver,
     * it is hard to verify the remote resolver was actually picked. One hackish way to assert
     * the remote resolver was OR was not picked based on the configuration, is to use the
     * environment resolver. Since the environment resolver is always returned as a fallback,
     * we can assert the remote resolver was not picked if `ProxyConfigurationResolver::resolve`
     * succeeds and returns an environment proxy configuration.
     * */
    EnvironmentProxySetter setter(http_env_proxy_server, https_env_proxy_server);

    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");
    config->setString("proxy.http", "");
    config->setString("proxy.http.resolver", "");
    config->setString("proxy.http.resolver.endpoint", "http://resolver:8080/hostname");

    // even tho proxy protocol / scheme is https, it should not be picked (prior to this PR, it would be picked)
    config->setString("proxy.http.resolver.proxy_scheme", "https");
    config->setString("proxy.http.resolver.proxy_port", "80");
    config->setString("proxy.http.resolver.proxy_cache_time", "10");

    context->setConfig(config);

    auto http_proxy_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS, *config)->resolve();

    /*
     * Asserts env proxy is used and not the remote resolver. If the remote resolver is picked, it is an error because
     * there is no `http` specification for remote resolver
     * */
    ASSERT_EQ(http_proxy_configuration.host, https_env_proxy_server.getHost());
    ASSERT_EQ(http_proxy_configuration.port, https_env_proxy_server.getPort());
    ASSERT_EQ(http_proxy_configuration.protocol, DB::ProxyConfiguration::protocolFromString(https_env_proxy_server.getScheme()));
}

// remote resolver is tricky to be tested in unit tests

template <bool DISABLE_TUNNELING_FOR_HTTPS_REQUESTS_OVER_HTTP_PROXY, bool STRING>
void test_tunneling(DB::ContextMutablePtr context)
{
    EnvironmentProxySetter setter(http_env_proxy_server, https_env_proxy_server);

    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    config->setString("proxy", "");

    if constexpr (STRING)
    {
        config->setString("proxy.disable_tunneling_for_https_requests_over_http_proxy", DISABLE_TUNNELING_FOR_HTTPS_REQUESTS_OVER_HTTP_PROXY ? "true" : "false");
    }
    else
    {
        config->setBool("proxy.disable_tunneling_for_https_requests_over_http_proxy", DISABLE_TUNNELING_FOR_HTTPS_REQUESTS_OVER_HTTP_PROXY);
    }

    context->setConfig(config);

    auto https_configuration = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::Protocol::HTTPS, *config)->resolve();

    ASSERT_EQ(https_configuration.tunneling, !DISABLE_TUNNELING_FOR_HTTPS_REQUESTS_OVER_HTTP_PROXY);
}

TEST_F(ProxyConfigurationResolverProviderTests, TunnelingForHTTPSRequestsOverHTTPProxySetting)
{
    test_tunneling<false, false>(context);
    test_tunneling<false, true>(context);
    test_tunneling<true, false>(context);
    test_tunneling<true, true>(context);
}

