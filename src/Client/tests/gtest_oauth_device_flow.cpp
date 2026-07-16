#include <gtest/gtest.h>

#include <Client/OAuthDeviceFlow.h>
#include <Common/Exception.h>

using namespace DB;

TEST(OAuthDeviceFlow, NormalizeIssuerStripsTrailingSlash)
{
    EXPECT_EQ(normalizeOAuthIssuerURL("https://login.example.com/"), "https://login.example.com");
    EXPECT_EQ(normalizeOAuthIssuerURL("https://login.example.com/tenant/v2.0/"), "https://login.example.com/tenant/v2.0");
    EXPECT_EQ(normalizeOAuthIssuerURL("https://login.example.com"), "https://login.example.com");
}

TEST(OAuthDeviceFlow, BuildDiscoveryURLsAppendStyle)
{
    const auto urls = buildOAuthDiscoveryURLs("https://auth.example.com");
    ASSERT_FALSE(urls.empty());
    EXPECT_EQ(urls[0], "https://auth.example.com/.well-known/openid-configuration");
    EXPECT_EQ(urls[1], "https://auth.example.com/.well-known/oauth-authorization-server");
}

TEST(OAuthDeviceFlow, BuildDiscoveryURLsWithIssuerPath)
{
    const auto urls = buildOAuthDiscoveryURLs("https://login.microsoftonline.com/TENANT/v2.0");
    ASSERT_GE(urls.size(), 2u);
    EXPECT_EQ(urls[0], "https://login.microsoftonline.com/TENANT/v2.0/.well-known/openid-configuration");

    bool found_rfc8414 = false;
    for (const auto & url : urls)
    {
        if (url == "https://login.microsoftonline.com/.well-known/oauth-authorization-server/TENANT/v2.0")
            found_rfc8414 = true;
    }
    EXPECT_TRUE(found_rfc8414);
}

TEST(OAuthDeviceFlow, ParseDiscoveryDocument)
{
    const std::string json = R"({
        "issuer": "https://login.example.com/",
        "device_authorization_endpoint": "https://login.example.com/oauth2/v1/device/authorize",
        "token_endpoint": "https://login.example.com/oauth2/v1/token"
    })";

    auto endpoints = parseOAuthDiscoveryDocument(json);
    ASSERT_TRUE(endpoints.has_value());
    EXPECT_EQ(endpoints->device_authorization_endpoint, "https://login.example.com/oauth2/v1/device/authorize");
    EXPECT_EQ(endpoints->token_endpoint, "https://login.example.com/oauth2/v1/token");
}

TEST(OAuthDeviceFlow, ParseDiscoveryDocumentRequiresDeviceEndpoint)
{
    const std::string json = R"({
        "token_endpoint": "https://login.example.com/oauth2/v1/token"
    })";
    EXPECT_FALSE(parseOAuthDiscoveryDocument(json).has_value());
    EXPECT_FALSE(parseOAuthDiscoveryDocument("not-json").has_value());
}

TEST(OAuthDeviceFlow, Auth0StyleFallback)
{
    const auto endpoints = auth0StyleOAuthEndpoints("https://auth.clickhouse.cloud/");
    EXPECT_EQ(endpoints.device_authorization_endpoint, "https://auth.clickhouse.cloud/oauth/device/code");
    EXPECT_EQ(endpoints.token_endpoint, "https://auth.clickhouse.cloud/oauth/token");
}

TEST(OAuthDeviceFlow, ApplyOverrides)
{
    OAuthDeviceFlowEndpoints base{
        "https://issuer.example/device",
        "https://issuer.example/token",
    };
    const auto overridden = applyOAuthEndpointOverrides(
        base,
        "https://login.microsoftonline.com/TENANT/oauth2/v2.0/devicecode",
        "https://login.microsoftonline.com/TENANT/oauth2/v2.0/token");

    EXPECT_EQ(overridden.device_authorization_endpoint, "https://login.microsoftonline.com/TENANT/oauth2/v2.0/devicecode");
    EXPECT_EQ(overridden.token_endpoint, "https://login.microsoftonline.com/TENANT/oauth2/v2.0/token");
}

TEST(OAuthDeviceFlow, ResolveVerificationURICompletePreferred)
{
    EXPECT_EQ(
        resolveDeviceVerificationURI("https://example.com/activate?user_code=ABCD", "https://example.com/activate", "ABCD"),
        "https://example.com/activate?user_code=ABCD");
}

TEST(OAuthDeviceFlow, ResolveVerificationURIFallbackAppendsUserCode)
{
    const std::string uri = resolveDeviceVerificationURI("", "https://example.com/activate", "ABCD-EFGH");
    EXPECT_NE(uri.find("https://example.com/activate"), std::string::npos);
    EXPECT_NE(uri.find("user_code=ABCD-EFGH"), std::string::npos);
}

TEST(OAuthDeviceFlow, ResolveVerificationURIMissingThrows)
{
    EXPECT_THROW(resolveDeviceVerificationURI("", "", "CODE"), Exception);
}
