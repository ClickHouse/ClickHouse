#include <gtest/gtest.h>

#include <Common/proxyConfigurationToPocoProxyConfig.h>

TEST(ProxyConfigurationToPocoProxyConfiguration, TestNoProxyHostRegexBuild)
{
    DB::ProxyConfiguration proxy_configuration;

    proxy_configuration.no_proxy_hosts = "localhost,127.0.0.1,some_other_domain:8080,sub-domain.domain.com";

    auto poco_proxy_configuration = DB::proxyConfigurationToPocoProxyConfig(proxy_configuration);

    ASSERT_EQ(poco_proxy_configuration.nonProxyHosts, R"((?:.*\.)?localhost|(?:.*\.)?127\.0\.0\.1|(?:.*\.)?some_other_domain\:8080|(?:.*\.)?sub\-domain\.domain\.com)");
}

TEST(ProxyConfigurationToPocoProxyConfiguration, TestNoProxyHostRegexBuildMatchAnything)
{
    DB::ProxyConfiguration proxy_configuration;

    proxy_configuration.no_proxy_hosts = "*";

    auto poco_proxy_configuration = DB::proxyConfigurationToPocoProxyConfig(proxy_configuration);

    ASSERT_EQ(poco_proxy_configuration.nonProxyHosts, "(.*?)");
}

TEST(ProxyConfigurationToPocoProxyConfiguration, TestNoProxyHostRegexBuildEmpty)
{
    DB::ProxyConfiguration proxy_configuration;

    proxy_configuration.no_proxy_hosts = "";

    auto poco_proxy_configuration = DB::proxyConfigurationToPocoProxyConfig(proxy_configuration);

    ASSERT_EQ(poco_proxy_configuration.nonProxyHosts, "");
}
