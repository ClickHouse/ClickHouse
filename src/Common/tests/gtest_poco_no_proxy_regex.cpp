#include <gtest/gtest.h>

#include <Common/proxyConfigurationToPocoProxyConfig.h>

TEST(ProxyConfigurationToPocoProxyConfiguration, TestNoProxyHostRegexBuild)
{
    ASSERT_EQ(
        DB::buildPocoNonProxyHosts("localhost,127.0.0.1,some_other_domain:8080,sub-domain.domain.com"),
        R"((?:.*\.)?localhost|(?:.*\.)?127\.0\.0\.1|(?:.*\.)?some_other_domain\:8080|(?:.*\.)?sub\-domain\.domain\.com)");
}

TEST(ProxyConfigurationToPocoProxyConfiguration, TestNoProxyHostRegexBuildMatchAnything)
{
    ASSERT_EQ(
        DB::buildPocoNonProxyHosts("*"),
        ".*");
}

TEST(ProxyConfigurationToPocoProxyConfiguration, TestNoProxyHostRegexBuildEmpty)
{
    ASSERT_EQ(
        DB::buildPocoNonProxyHosts(""),
        "");
}
