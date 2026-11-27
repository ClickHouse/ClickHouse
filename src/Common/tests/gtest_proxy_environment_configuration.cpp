#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/tests/gtest_helper_functions.h>
#include <Common/proxyConfigurationToPocoProxyConfig.h>
#include <Poco/URI.h>

namespace DB
{

TEST(EnvironmentProxyConfigurationResolver, TestHTTPandHTTPS)
{
    const auto http_proxy_server = Poco::URI(EnvironmentProxySetter::HTTP_PROXY);
    const auto https_proxy_server = Poco::URI(EnvironmentProxySetter::HTTPS_PROXY);

    std::string poco_no_proxy_regex = buildPocoNonProxyHosts(EnvironmentProxySetter::NO_PROXY);

    EnvironmentProxySetter setter;

    EnvironmentProxyConfigurationResolver http_resolver(ProxyConfiguration::Protocol::HTTP);

    auto http_configuration = http_resolver.resolve();

    ASSERT_EQ(http_configuration.host, http_proxy_server.getHost());
    ASSERT_EQ(http_configuration.port, http_proxy_server.getPort());
    ASSERT_EQ(http_configuration.protocol, ProxyConfiguration::protocolFromString(http_proxy_server.getScheme()));
    ASSERT_EQ(http_configuration.no_proxy_hosts, poco_no_proxy_regex);

    EnvironmentProxyConfigurationResolver https_resolver(ProxyConfiguration::Protocol::HTTPS);

    auto https_configuration = https_resolver.resolve();

    ASSERT_EQ(https_configuration.host, https_proxy_server.getHost());
    ASSERT_EQ(https_configuration.port, https_proxy_server.getPort());
    ASSERT_EQ(https_configuration.protocol, ProxyConfiguration::protocolFromString(https_proxy_server.getScheme()));
    ASSERT_EQ(https_configuration.no_proxy_hosts, poco_no_proxy_regex);
}

}
