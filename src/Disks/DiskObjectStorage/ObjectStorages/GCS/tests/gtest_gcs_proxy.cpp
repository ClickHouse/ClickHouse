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

#endif
