#include <config.h>

#if USE_JWT_CPP && USE_SSL

#include <Client/OAuthLogin.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/scope_guard_safe.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>

using namespace DB;

namespace
{

namespace fs = std::filesystem;

/// Write content to a temp file and return its path. The caller owns the file.
std::string writeTempFile(const std::string & content)
{
    const fs::path tmp = fs::temp_directory_path() / fs::path("gtest_oauth_XXXXXX");
    // std::tmpnam is deprecated — build a unique name with mkstemp.
    std::string tmpl = tmp.string();
    int fd = mkstemp(tmpl.data());
    if (fd < 0)
        throw std::runtime_error("mkstemp failed");
    close(fd);

    std::ofstream f(tmpl, std::ios::trunc);
    f << content;
    return tmpl;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// loadOAuthCredentials — valid "installed" format
// ---------------------------------------------------------------------------

TEST(OAuthLogin, LoadInstalledFormat)
{
    const std::string json = R"({
        "installed": {
            "client_id": "test-client-id",
            "client_secret": "test-secret",
            "auth_uri": "https://auth.example.com/auth",
            "token_uri": "https://auth.example.com/token",
            "redirect_uris": ["http://localhost"]
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_EQ(creds.client_id, "test-client-id");
    EXPECT_EQ(creds.client_secret, "test-secret");
    EXPECT_EQ(creds.auth_uri, "https://auth.example.com/auth");
    EXPECT_EQ(creds.token_uri, "https://auth.example.com/token");
    EXPECT_TRUE(creds.device_auth_uri.empty());
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — valid "web" format
// ---------------------------------------------------------------------------

TEST(OAuthLogin, LoadWebFormat)
{
    const std::string json = R"({
        "web": {
            "client_id": "web-client",
            "client_secret": "web-secret",
            "auth_uri": "https://web.example.com/auth",
            "token_uri": "https://web.example.com/token"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_EQ(creds.client_id, "web-client");
    EXPECT_EQ(creds.client_secret, "web-secret");
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — optional device_authorization_uri is loaded
// ---------------------------------------------------------------------------

TEST(OAuthLogin, LoadDeviceAuthUri)
{
    const std::string json = R"({
        "installed": {
            "client_id": "x",
            "client_secret": "y",
            "auth_uri": "https://a.example.com/auth",
            "token_uri": "https://a.example.com/token",
            "device_authorization_uri": "https://a.example.com/device"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_EQ(creds.device_auth_uri, "https://a.example.com/device");
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — missing top-level key throws BAD_ARGUMENTS
// ---------------------------------------------------------------------------

TEST(OAuthLogin, MissingTopLevelKey)
{
    const std::string json = R"({ "other_key": {} })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    EXPECT_THROW(loadOAuthCredentials(path), Exception);
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — public-client config (no client_secret) loads OK
//
// Per RFC 6749 §2.1 / RFC 8252 §8.4 native OIDC clients are typically
// registered as public clients with no secret; the flow is protected by PKCE
// (auth-code) or the device_code (device flow). The credential loader must
// not hard-require client_secret, otherwise valid public-client registrations
// cannot be used. This is the regression guard for that policy: the absence
// of the field is silently accepted, and the in-memory secret stays empty so
// the downstream POST builders omit the parameter rather than sending an
// empty value (which several IdPs reject as invalid_client).
// ---------------------------------------------------------------------------

TEST(OAuthLogin, LoadPublicClientNoSecret)
{
    const std::string json = R"({
        "installed": {
            "client_id": "public-client-id",
            "auth_uri": "https://auth.example.com/auth",
            "token_uri": "https://auth.example.com/token"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_EQ(creds.client_id, "public-client-id");
    EXPECT_TRUE(creds.client_secret.empty());
    EXPECT_EQ(creds.auth_uri, "https://auth.example.com/auth");
    EXPECT_EQ(creds.token_uri, "https://auth.example.com/token");
}

// Empty-string client_secret is treated identically to an absent field: load
// succeeds and the in-memory value is empty, so the downstream POST bodies
// omit the form parameter. Without this property a credential file written
// by a tool that defaults the field to "" would produce invalid_client at
// the IdP rather than a working public-client request.
TEST(OAuthLogin, LoadPublicClientEmptySecret)
{
    const std::string json = R"({
        "installed": {
            "client_id": "public-client-id",
            "client_secret": "",
            "auth_uri": "https://auth.example.com/auth",
            "token_uri": "https://auth.example.com/token"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_TRUE(creds.client_secret.empty());
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — missing required field throws BAD_ARGUMENTS
// ---------------------------------------------------------------------------

TEST(OAuthLogin, MissingClientId)
{
    const std::string json = R"({
        "installed": {
            "client_secret": "s",
            "auth_uri": "https://a.example.com/auth",
            "token_uri": "https://a.example.com/token"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    EXPECT_THROW(loadOAuthCredentials(path), Exception);
}

TEST(OAuthLogin, MissingTokenUri)
{
    const std::string json = R"({
        "installed": {
            "client_id": "c",
            "client_secret": "s",
            "auth_uri": "https://a.example.com/auth"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    EXPECT_THROW(loadOAuthCredentials(path), Exception);
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — file not found throws BAD_ARGUMENTS
// ---------------------------------------------------------------------------

TEST(OAuthLogin, FileNotFound)
{
    EXPECT_THROW(loadOAuthCredentials("/nonexistent/path/oauth_client.json"), Exception);
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — invalid JSON throws BAD_ARGUMENTS
// ---------------------------------------------------------------------------

TEST(OAuthLogin, InvalidJson)
{
    auto path = writeTempFile("not valid json {{{");
    SCOPE_EXIT({ fs::remove(path); });

    EXPECT_THROW(loadOAuthCredentials(path), Exception);
}

// ---------------------------------------------------------------------------
// loadOAuthCredentials — optional "issuer" field is loaded
// ---------------------------------------------------------------------------

TEST(OAuthLogin, LoadIssuerField)
{
    const std::string json = R"({
        "installed": {
            "client_id": "x",
            "client_secret": "y",
            "auth_uri": "https://a.example.com/auth",
            "token_uri": "https://a.example.com/token",
            "issuer": "https://a.example.com"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_EQ(creds.issuer, "https://a.example.com");
}

TEST(OAuthLogin, IssuerFieldAbsent)
{
    const std::string json = R"({
        "installed": {
            "client_id": "x",
            "client_secret": "y",
            "auth_uri": "https://a.example.com/auth",
            "token_uri": "https://a.example.com/token"
        }
    })";

    auto path = writeTempFile(json);
    SCOPE_EXIT({ fs::remove(path); });

    auto creds = loadOAuthCredentials(path);
    EXPECT_TRUE(creds.issuer.empty());
}

// ---------------------------------------------------------------------------
// PKCE building blocks
//
// generatePKCE() is in the anonymous namespace so we test its constituent
// operations (base64url encoding and SHA-256) directly. This verifies the
// exact properties that RFC 7636 §4 requires of the verifier and challenge.
// ---------------------------------------------------------------------------

TEST(OAuthLogin, Base64UrlEncodingProperties)
{
    // 32 bytes → 43 base64url chars (no padding, RFC 7636 §4.1 requires 43-128).
    const std::string raw(32, '\xAB');
    const std::string encoded = base64Encode(raw, /*url_encoding=*/true, /*no_padding=*/true);

    EXPECT_EQ(encoded.size(), 43u);

    // Must contain only URL-safe base64 chars: A-Z a-z 0-9 - _
    const bool all_safe = std::all_of(encoded.begin(), encoded.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '-' || c == '_';
    });
    EXPECT_TRUE(all_safe) << "base64url output contains non-URL-safe characters: " << encoded;

    // Must NOT contain padding or standard base64 symbols.
    EXPECT_EQ(encoded.find('='), std::string::npos);
    EXPECT_EQ(encoded.find('+'), std::string::npos);
    EXPECT_EQ(encoded.find('/'), std::string::npos);
}

TEST(OAuthLogin, PKCEChallengeDerivation)
{
    // SHA256(verifier) encodes to 32 bytes; base64url(32 bytes) = 43 chars.
    const std::string verifier = base64Encode(std::string(32, '\x01'), true, true);
    const std::string sha = encodeSHA256(verifier);
    EXPECT_EQ(sha.size(), 32u);

    const std::string challenge = base64Encode(sha, true, true);
    EXPECT_EQ(challenge.size(), 43u);

    // Challenge must differ from verifier.
    EXPECT_NE(challenge, verifier);

    // Challenge must be deterministic for the same verifier.
    EXPECT_EQ(base64Encode(encodeSHA256(verifier), true, true), challenge);

    // Different verifiers must produce different challenges.
    const std::string verifier2 = base64Encode(std::string(32, '\x02'), true, true);
    EXPECT_NE(base64Encode(encodeSHA256(verifier2), true, true), challenge);
}

#endif // USE_JWT_CPP && USE_SSL
