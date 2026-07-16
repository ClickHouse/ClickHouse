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
        "token_endpoint": "https://login.example.com/oauth2/v1/token",
        "grant_types_supported": ["authorization_code", "urn:ietf:params:oauth:grant-type:device_code"]
    })";

    auto endpoints = parseOAuthDiscoveryDocument(json);
    ASSERT_TRUE(endpoints.has_value());
    EXPECT_EQ(endpoints->device_authorization_endpoint, "https://login.example.com/oauth2/v1/device/authorize");
    EXPECT_EQ(endpoints->token_endpoint, "https://login.example.com/oauth2/v1/token");
}

TEST(OAuthDeviceFlow, ParseDiscoveryDocumentOmitsGrantTypesSupported)
{
    const std::string json = R"({
        "device_authorization_endpoint": "https://login.example.com/device",
        "token_endpoint": "https://login.example.com/token"
    })";
    EXPECT_TRUE(discoverySupportsDeviceCodeGrant(json));
    ASSERT_TRUE(parseOAuthDiscoveryDocument(json).has_value());
}

TEST(OAuthDeviceFlow, ParseDiscoveryDocumentRejectsMissingDeviceGrant)
{
    const std::string json = R"({
        "device_authorization_endpoint": "https://login.example.com/device",
        "token_endpoint": "https://login.example.com/token",
        "grant_types_supported": ["authorization_code"]
    })";
    EXPECT_FALSE(parseOAuthDiscoveryDocument(json).has_value());
    EXPECT_FALSE(discoverySupportsDeviceCodeGrant(json));
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

TEST(OAuthDeviceFlow, ApplyOverridesPartialKeepsOtherEndpoint)
{
    OAuthDeviceFlowEndpoints base{
        "https://issuer.example/device",
        "https://issuer.example/token",
    };
    const auto device_only = applyOAuthEndpointOverrides(base, "https://override/device", "");
    EXPECT_EQ(device_only.device_authorization_endpoint, "https://override/device");
    EXPECT_EQ(device_only.token_endpoint, "https://issuer.example/token");

    const auto token_only = applyOAuthEndpointOverrides(base, "", "https://override/token");
    EXPECT_EQ(token_only.device_authorization_endpoint, "https://issuer.example/device");
    EXPECT_EQ(token_only.token_endpoint, "https://override/token");
}

TEST(OAuthDeviceFlow, ValidateEndpointOverridePair)
{
    EXPECT_NO_THROW(validateOAuthEndpointOverridePair("", ""));
    EXPECT_NO_THROW(validateOAuthEndpointOverridePair("https://a/device", "https://a/token"));
    EXPECT_THROW(validateOAuthEndpointOverridePair("https://a/device", ""), Exception);
    EXPECT_THROW(validateOAuthEndpointOverridePair("", "https://a/token"), Exception);
}

TEST(OAuthDeviceFlow, EncodeFormBodyEncodesGrantTypeAndOmitsEmpty)
{
    const std::string body = buildFormUrlEncodedBody({
        {"grant_type", oauth_device_code_grant_type},
        {"client_id", "abc:def"},
        {"scope", ""},
    });

    EXPECT_NE(body.find("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code"), std::string::npos);
    EXPECT_NE(body.find("client_id=abc%3Adef"), std::string::npos);
    EXPECT_EQ(body.find("scope="), std::string::npos);
}

TEST(OAuthDeviceFlow, EffectiveScopeUsesDefaultWhenEmpty)
{
    EXPECT_EQ(effectiveOAuthDeviceCodeScope(""), default_oauth_device_code_scope);
    EXPECT_EQ(effectiveOAuthDeviceCodeScope("openid offline_access"), "openid offline_access");
}

TEST(OAuthDeviceFlow, ParseOAuthErrorResponse)
{
    auto error = parseOAuthErrorResponse(R"({"error":"access_denied","error_description":"user said no"})");
    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(error->error, "access_denied");
    EXPECT_EQ(formatOAuthError(*error), "access_denied: user said no");
    EXPECT_FALSE(parseOAuthErrorResponse("not-json").has_value());
}

TEST(OAuthDeviceFlow, FormatOAuthErrorFallsBackToStatusAndBody)
{
    EXPECT_EQ(formatOAuthError("plain failure", 400, "Bad Request"), "400 Bad Request: plain failure");
    EXPECT_EQ(formatOAuthError("", 503, "Service Unavailable"), "503 Service Unavailable");
    EXPECT_EQ(
        formatOAuthError(R"({"error":"invalid_client","error_description":"bad secret"})", 401, "Unauthorized"),
        "invalid_client: bad secret");
}

TEST(OAuthDeviceFlow, FormatDeviceLoginInstructionsAlwaysShowsShortURI)
{
    const std::string text = formatDeviceLoginInstructions(
        "https://example.com/device",
        "WDJB-MJHT",
        "https://example.com/device?user_code=WDJB-MJHT");

    EXPECT_NE(text.find("https://example.com/device\n"), std::string::npos);
    EXPECT_NE(text.find("WDJB-MJHT"), std::string::npos);
    EXPECT_NE(text.find("https://example.com/device?user_code=WDJB-MJHT"), std::string::npos);
}

TEST(OAuthDeviceFlow, FormatDeviceLoginInstructionsWithoutCompleteOmitsShortcut)
{
    const std::string text = formatDeviceLoginInstructions("https://example.com/device", "ABCD", "");
    EXPECT_NE(text.find("https://example.com/device"), std::string::npos);
    EXPECT_NE(text.find("ABCD"), std::string::npos);
    EXPECT_EQ(text.find("Shortcut URL"), std::string::npos);
}

TEST(OAuthDeviceFlow, FormatDeviceLoginInstructionsRequiresURIAndCode)
{
    EXPECT_THROW(formatDeviceLoginInstructions("", "CODE", ""), Exception);
    EXPECT_THROW(formatDeviceLoginInstructions("https://example.com/device", "", ""), Exception);
}

TEST(OAuthDeviceFlow, BrowserURLPrefersComplete)
{
    EXPECT_EQ(
        browserVerificationURL("https://example.com/device?user_code=A", "https://example.com/device"),
        "https://example.com/device?user_code=A");
    EXPECT_EQ(browserVerificationURL("", "https://example.com/device"), "https://example.com/device");
}

TEST(OAuthDeviceFlow, ConnectionFailureBackoff)
{
    EXPECT_EQ(nextPollingIntervalAfterConnectionFailure(5), 10);
    EXPECT_EQ(nextPollingIntervalAfterConnectionFailure(40), 60);
    EXPECT_EQ(nextPollingIntervalAfterConnectionFailure(60), 60);
    EXPECT_EQ(nextPollingIntervalAfterConnectionFailure(0), 5);
}

TEST(OAuthDeviceFlow, EvaluateTokenPollAuthorizationPending)
{
    const auto decision = evaluateDeviceTokenPollFailure(
        R"({"error":"authorization_pending"})", 400, "Bad Request", 5);
    EXPECT_EQ(decision.action, DeviceTokenPollAction::ContinuePending);
    EXPECT_EQ(decision.interval_seconds, 5);
}

TEST(OAuthDeviceFlow, EvaluateTokenPollSlowDown)
{
    const auto decision = evaluateDeviceTokenPollFailure(R"({"error":"slow_down"})", 400, "Bad Request", 5);
    EXPECT_EQ(decision.action, DeviceTokenPollAction::ContinueSlowDown);
    EXPECT_EQ(decision.interval_seconds, 10);
}

TEST(OAuthDeviceFlow, EvaluateTokenPollAccessDenied)
{
    const auto decision = evaluateDeviceTokenPollFailure(
        R"({"error":"access_denied","error_description":"user rejected"})", 400, "Bad Request", 5);
    EXPECT_EQ(decision.action, DeviceTokenPollAction::FailAccessDenied);
    EXPECT_EQ(decision.message, "access_denied: user rejected");
}

TEST(OAuthDeviceFlow, EvaluateTokenPollExpiredToken)
{
    const auto decision = evaluateDeviceTokenPollFailure(
        R"({"error":"expired_token","error_description":"code expired"})", 400, "Bad Request", 5);
    EXPECT_EQ(decision.action, DeviceTokenPollAction::FailExpiredToken);
    EXPECT_EQ(decision.message, "expired_token: code expired");
}

TEST(OAuthDeviceFlow, EvaluateTokenPollOtherError)
{
    const auto decision = evaluateDeviceTokenPollFailure(
        R"({"error":"invalid_grant","error_description":"bad code"})", 400, "Bad Request", 5);
    EXPECT_EQ(decision.action, DeviceTokenPollAction::FailOther);
    EXPECT_EQ(decision.message, "invalid_grant: bad code");
}

TEST(OAuthDeviceFlow, EvaluateTokenPollNonJsonError)
{
    const auto decision = evaluateDeviceTokenPollFailure("gateway timeout", 504, "Gateway Timeout", 5);
    EXPECT_EQ(decision.action, DeviceTokenPollAction::FailOther);
    EXPECT_EQ(decision.message, "504 Gateway Timeout: gateway timeout");
}

TEST(OAuthClientAuth, ParseMethods)
{
    EXPECT_EQ(parseOAuthClientAuthMethod("", ""), OAuthClientAuthMethod::None);
    EXPECT_EQ(parseOAuthClientAuthMethod("", "basic"), OAuthClientAuthMethod::None);
    EXPECT_EQ(parseOAuthClientAuthMethod("secret", ""), OAuthClientAuthMethod::Basic);
    EXPECT_EQ(parseOAuthClientAuthMethod("secret", "basic"), OAuthClientAuthMethod::Basic);
    EXPECT_EQ(parseOAuthClientAuthMethod("secret", "BASIC"), OAuthClientAuthMethod::Basic);
    EXPECT_EQ(parseOAuthClientAuthMethod("secret", "post"), OAuthClientAuthMethod::Post);
    EXPECT_THROW(parseOAuthClientAuthMethod("secret", "digest"), Exception);
}

TEST(OAuthClientAuth, AppendClientSecretPost)
{
    EXPECT_EQ(appendClientSecretPost("client_id=abc", "s:ecret"), "client_id=abc&client_secret=s%3Aecret");
    EXPECT_EQ(appendClientSecretPost("", "secret"), "client_secret=secret");
    EXPECT_EQ(appendClientSecretPost("client_id=abc", ""), "client_id=abc");
}
