#include <gtest/gtest.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/tests/gtest_helper_functions.h>
#include <Common/proxyConfigurationToPocoProxyConfig.h>
#include <Poco/URI.h>

namespace DB
{

namespace
{
    auto http_proxy_server = Poco::URI("http://proxy_server:3128");
    auto https_proxy_server = Poco::URI("https://proxy_server:3128");
}

TEST(EnvironmentProxyConfigurationResolver, TestHTTPandHTTPS)
{
    std::string no_proxy_string = "localhost,,127.0.0.1,some_other_domain,,,, last_domain,";
    std::string poco_no_proxy_regex = buildPocoNonProxyHosts(no_proxy_string);
    EnvironmentProxySetter setter(http_proxy_server, https_proxy_server, no_proxy_string);

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
