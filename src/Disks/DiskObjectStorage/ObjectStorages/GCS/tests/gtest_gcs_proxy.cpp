#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <Common/ProxyListConfigurationResolver.h>
#include <gtest/gtest.h>

using namespace DB;

/// The native GCS client hands the Poco-based REST transport a callback instead of a fixed proxy, so
/// that every request resolves the proxy again — a rotating list has to rotate, and a resolver that
/// can change its mind (the remote one) has to be asked each time.

TEST(GCSProxy, NoResolverMeansNoProvider)
{
    EXPECT_FALSE(static_cast<bool>(makeGCSProxyConfigProvider(nullptr)));
}

TEST(GCSProxy, TranslatesTheResolvedConfiguration)
{
    auto resolver = std::make_shared<ProxyListConfigurationResolver>(
        std::vector<Poco::URI>{Poco::URI("http://proxy1:3128")},
        ProxyConfiguration::Protocol::HTTPS,
        R"(localhost|127\.0\.0\.1)");

    auto provider = makeGCSProxyConfigProvider(resolver);
    ASSERT_TRUE(static_cast<bool>(provider));

    const auto proxy = provider();
    EXPECT_EQ(proxy.host, "proxy1");
    EXPECT_EQ(proxy.port, 3128);
    EXPECT_EQ(proxy.protocol, "http");
    EXPECT_EQ(proxy.originalRequestProtocol, "https");
    /// An HTTPS request over an HTTP proxy goes through a CONNECT tunnel.
    EXPECT_TRUE(proxy.tunnel);
    EXPECT_EQ(proxy.nonProxyHosts, R"(localhost|127\.0\.0\.1)");
}

TEST(GCSProxy, ResolvesOnEveryCall)
{
    auto resolver = std::make_shared<ProxyListConfigurationResolver>(
        std::vector<Poco::URI>{Poco::URI("http://proxy1:3128"), Poco::URI("http://proxy2:3128")},
        ProxyConfiguration::Protocol::HTTP,
        "");

    auto provider = makeGCSProxyConfigProvider(resolver);
    ASSERT_TRUE(static_cast<bool>(provider));

    EXPECT_EQ(provider().host, "proxy1");
    EXPECT_EQ(provider().host, "proxy2");
    EXPECT_EQ(provider().host, "proxy1");
}

TEST(GCSProxy, EmptyListMeansDirectConnection)
{
    auto resolver = std::make_shared<ProxyListConfigurationResolver>(
        std::vector<Poco::URI>{}, ProxyConfiguration::Protocol::HTTPS, "");

    auto provider = makeGCSProxyConfigProvider(resolver);
    ASSERT_TRUE(static_cast<bool>(provider));

    /// An empty host is what the transport reads as "no proxy for this request".
    EXPECT_TRUE(provider().host.empty());
}

/// Two storages that resolve their proxy differently must not share one client: the transport
/// options are baked into it, so a server-side `RewriteObject` between them would take another
/// storage's proxy.
TEST(GCSProxy, DifferentResolversAreDifferentClients)
{
    GCSObjectStorageSettings left;
    GCSObjectStorageSettings right;
    /// Application Default Credentials are intentionally never treated as identical: their identity
    /// comes from external mutable state. Use anonymous credentials to isolate proxy identity here.
    left.no_sign_request = true;
    right.no_sign_request = true;
    EXPECT_TRUE(left.describesSameClientAs(right));

    left.proxy_resolver = std::make_shared<ProxyListConfigurationResolver>(
        std::vector<Poco::URI>{Poco::URI("http://proxy1:3128")}, ProxyConfiguration::Protocol::HTTPS, "");
    EXPECT_FALSE(left.describesSameClientAs(right));

    right.proxy_resolver = std::make_shared<ProxyListConfigurationResolver>(
        std::vector<Poco::URI>{Poco::URI("http://proxy1:3128")}, ProxyConfiguration::Protocol::HTTPS, "");
    EXPECT_FALSE(left.describesSameClientAs(right));

    right.proxy_resolver = left.proxy_resolver;
    EXPECT_TRUE(left.describesSameClientAs(right));
}

/// The resolver also has to be told when a request through the proxy it handed out failed: that is what
/// invalidates a cached bad proxy (`RemoteProxyConfigurationResolver`) instead of retrying it until the
/// TTL of its list expires. A recording resolver stands in for it here — `ProxyListConfigurationResolver`
/// rotates its list and ignores the report.
namespace
{

class RecordingProxyResolver : public ProxyConfigurationResolver
{
public:
    RecordingProxyResolver() : ProxyConfigurationResolver(ProxyConfiguration::Protocol::HTTPS) { }

    ProxyConfiguration resolve() override { return resolved; }

    void errorReport(const ProxyConfiguration & config) override { reported.push_back(config); }

    ProxyConfiguration resolved;
    std::vector<ProxyConfiguration> reported;
};

}

TEST(GCSProxy, NoResolverMeansNoErrorReporter)
{
    EXPECT_FALSE(static_cast<bool>(makeGCSProxyErrorReporter(nullptr)));
}

TEST(GCSProxy, ReportsTheFailedProxyBackToTheResolver)
{
    auto resolver = std::make_shared<RecordingProxyResolver>();
    auto report = makeGCSProxyErrorReporter(resolver);
    ASSERT_TRUE(static_cast<bool>(report));

    Poco::Net::HTTPClientSession::ProxyConfig proxy;
    proxy.host = "proxy1";
    proxy.port = 3128;
    proxy.protocol = "https";
    report(proxy);

    ASSERT_EQ(resolver->reported.size(), 1);
    EXPECT_EQ(resolver->reported[0].host, "proxy1");
    EXPECT_EQ(resolver->reported[0].port, 3128);
    EXPECT_EQ(resolver->reported[0].protocol, ProxyConfiguration::Protocol::HTTPS);
}

TEST(GCSProxy, ReportsNothingForARequestThatUsedNoProxy)
{
    auto resolver = std::make_shared<RecordingProxyResolver>();
    auto report = makeGCSProxyErrorReporter(resolver);
    ASSERT_TRUE(static_cast<bool>(report));

    /// A default-constructed configuration is how the transport says "this request went direct".
    report(Poco::Net::HTTPClientSession::ProxyConfig{});
    /// A protocol no resolved proxy can have cannot match a cached one either, so there is nothing to
    /// invalidate and nothing is reported.
    Poco::Net::HTTPClientSession::ProxyConfig socks;
    socks.host = "proxy1";
    socks.port = 1080;
    socks.protocol = "socks5";
    report(socks);

    EXPECT_TRUE(resolver->reported.empty());
}

/// The keep-alive policy of the pooled connections is part of the client: two storages that bound the
/// lifetime of a connection differently cannot share one.
TEST(GCSProxy, DifferentKeepAlivePolicyIsADifferentClient)
{
    GCSObjectStorageSettings left;
    GCSObjectStorageSettings right;
    left.no_sign_request = true;
    right.no_sign_request = true;
    ASSERT_TRUE(left.describesSameClientAs(right));

    left.http_keep_alive_timeout = right.http_keep_alive_timeout + 1;
    EXPECT_FALSE(left.describesSameClientAs(right));

    right.http_keep_alive_timeout = left.http_keep_alive_timeout;
    ASSERT_TRUE(left.describesSameClientAs(right));

    left.http_keep_alive_max_requests = right.http_keep_alive_max_requests + 1;
    EXPECT_FALSE(left.describesSameClientAs(right));
}

#endif
