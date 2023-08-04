#include "config.h"

#if USE_AWS_S3

#include <IO/S3/ProxyConfigurationResolverAdapter.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <gtest/gtest.h>

struct ProxyConfigurationResolverMock : public DB::ProxyConfigurationResolver
{
    ProxyConfigurationResolverMock(std::string host_, std::string scheme_, uint16_t port_)
    : host(std::move(host_)), scheme(std::move(scheme_)), port(port_)
    {}

    DB::ProxyConfiguration resolve() override
    {
        return DB::ProxyConfiguration {
            host,
            DB::ProxyConfiguration::fromString(scheme),
            port
        };
    }

    void errorReport(const DB::ProxyConfiguration &) override {}

private:
    std::string host;
    std::string scheme;
    uint16_t port;
};

TEST(ProxyResolverAdapter, TestAdapt)
{
    auto resolver = std::make_shared<ProxyConfigurationResolverMock>(
        "abc",
        "http",
        3128
    );

    auto resolver_adapter = DB::S3::ProxyConfigurationResolverAdapter(resolver);

    auto request = Aws::Http::Standard::StandardHttpRequest("https://example.com", Aws::Http::HttpMethod::HTTP_GET);

    auto config = resolver_adapter.getConfiguration(request);

    ASSERT_EQ(config.proxy_host, "abc");
    ASSERT_EQ(config.proxy_scheme, Aws::Http::SchemeMapper::FromString("http"));
    ASSERT_EQ(config.proxy_port, 3128);
}

#endif
