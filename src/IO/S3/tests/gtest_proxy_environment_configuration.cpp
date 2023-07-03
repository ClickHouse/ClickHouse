#include <gtest/gtest.h>

#include <IO/S3/ProxyEnvironmentConfiguration.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

namespace
{
    auto proxy_server = Poco::URI("http://proxy_server:3128");
    auto http_request = Aws::Http::Standard::StandardHttpRequest("http://google.com.br", Aws::Http::HttpMethod::HTTP_GET);
    auto https_request = Aws::Http::Standard::StandardHttpRequest("https://google.com.br", Aws::Http::HttpMethod::HTTP_GET);
}

TEST(ProxyEnvironmentConfiguration, TestHTTP)
{
    setenv("http_proxy", proxy_server.toString().c_str(), 1);

    DB::S3::ProxyEnvironmentConfiguration proxy_config;

    auto configuration = proxy_config.getConfiguration(http_request);

    ASSERT_EQ(configuration.proxy_host, proxy_server.getHost());
    ASSERT_EQ(configuration.proxy_port, proxy_server.getPort());
    ASSERT_EQ(Aws::Http::SchemeMapper::ToString(configuration.proxy_scheme), proxy_server.getScheme());
}

TEST(ProxyEnvironmentConfiguration, TestHTTPNoEnv)
{
    unsetenv("http_proxy");

    auto configuration = DB::S3::ProxyEnvironmentConfiguration().getConfiguration(http_request);

    ASSERT_EQ(configuration.proxy_host, "");
    ASSERT_EQ(configuration.proxy_port, 0);
}

TEST(ProxyEnvironmentConfiguration, TestHTTPs)
{
    setenv("https_proxy", proxy_server.toString().c_str(), 1);

    auto configuration = DB::S3::ProxyEnvironmentConfiguration().getConfiguration(https_request);

    ASSERT_EQ(configuration.proxy_host, proxy_server.getHost());
    ASSERT_EQ(configuration.proxy_port, proxy_server.getPort());
    ASSERT_EQ(Aws::Http::SchemeMapper::ToString(configuration.proxy_scheme), proxy_server.getScheme());
}

TEST(ProxyEnvironmentConfiguration, TestHTTPsNoEnv)
{
    unsetenv("https_proxy");

    auto configuration = DB::S3::ProxyEnvironmentConfiguration().getConfiguration(https_request);

    ASSERT_EQ(configuration.proxy_host, "");
    ASSERT_EQ(configuration.proxy_port, 0);
}

