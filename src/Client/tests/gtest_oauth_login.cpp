#include <config.h>

#if USE_JWT_CPP && USE_SSL

#include <Client/OAuthLogin.h>
#include <Client/OAuthFlowRunner.h>
#include <Client/OAuthLoopbackServer.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/scope_guard_safe.h>
#include <gtest/gtest.h>

#include <Poco/AutoPtr.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Timespan.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

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

// ---------------------------------------------------------------------------
// buildDeviceAuthorizationRequestBody — RFC 8628 §3.1 client authentication.
// Regression guard for the confidential-client device flow: the device
// authorization request must carry `client_secret` when one is configured
// (otherwise a confidential IdP client rejects the very first request with
// `invalid_client`), and must omit the parameter entirely for public clients.
// ---------------------------------------------------------------------------

TEST(OAuthLogin, DeviceAuthorizationBodyIncludesClientSecret)
{
    OAuthCredentials creds;
    creds.client_id = "demo-confidential";
    creds.client_secret = "t0p-s3cret";

    const std::string body = buildDeviceAuthorizationRequestBody(creds, "openid");
    EXPECT_EQ(body, "client_id=demo-confidential&scope=openid&client_secret=t0p-s3cret");
}

TEST(OAuthLogin, DeviceAuthorizationBodyOmitsSecretForPublicClient)
{
    OAuthCredentials creds;
    creds.client_id = "demo-public";

    const std::string body = buildDeviceAuthorizationRequestBody(creds, "openid");
    EXPECT_EQ(body, "client_id=demo-public&scope=openid");
    // An empty value is not equivalent to omission and is rejected by several
    // IdPs as invalid_client — the parameter must be absent altogether.
    EXPECT_EQ(body.find("client_secret"), std::string::npos);
}

// ---------------------------------------------------------------------------
// copyStreamWithLimit — bounded read of untrusted OAuth/OIDC responses.
// Regression guard for the unbounded OIDC discovery read (memory-exhaustion
// DoS): the browser/device login HTTP reads must cap the body size.
// ---------------------------------------------------------------------------

TEST(OAuthLogin, CopyStreamWithLimitAcceptsWithinLimit)
{
    std::istringstream in(std::string("hello world"));
    std::string out;
    copyStreamWithLimit(in, out, 1024);
    EXPECT_EQ(out, "hello world");
}

TEST(OAuthLogin, CopyStreamWithLimitAcceptsExactBoundary)
{
    // A body of exactly max_bytes is accepted; only strictly larger fails.
    std::istringstream in(std::string(1024, 'x'));
    std::string out;
    copyStreamWithLimit(in, out, 1024);
    EXPECT_EQ(out.size(), 1024u);
}

TEST(OAuthLogin, CopyStreamWithLimitRejectsOversized)
{
    // A body larger than the limit must fail fast instead of buffering the
    // whole (potentially unbounded) response into memory.
    std::istringstream in(std::string(2048, 'x'));
    std::string out;
    EXPECT_THROW(copyStreamWithLimit(in, out, 1024), DB::Exception);
}

// ---------------------------------------------------------------------------
// Loopback callback server — must not leak the auth URL / CSRF state.
// Regression guard for the `/start` redirect that disclosed the full
// authorization URL (including `state`) to any local process.
// ---------------------------------------------------------------------------

namespace
{

struct RunningCallbackServer
{
    OAuthCallbackState state;
    Poco::Net::ServerSocket server_socket;
    Poco::Net::HTTPServer server;
    uint16_t port;

    explicit RunningCallbackServer(const std::string & expected_state = "")
        : server_socket(makeSocket())
        , server(createOAuthCallbackHandlerFactory(state), server_socket, makeParams())
        , port(server_socket.address().port())
    {
        /// Set before `start`, so the handler thread reads it race-free.
        state.expected_state = expected_state;
        server.start();
    }

    ~RunningCallbackServer() { server.stop(); }

    static Poco::Net::ServerSocket makeSocket()
    {
        Poco::Net::ServerSocket socket;
        socket.bind(Poco::Net::SocketAddress("127.0.0.1", 0), /*reuse_address=*/true);
        socket.listen(4);
        return socket;
    }

    static Poco::Net::HTTPServerParams * makeParams()
    {
        auto * params = new Poco::Net::HTTPServerParams();
        params->setMaxQueued(4);
        params->setMaxThreads(1);
        return params;
    }
};

struct HttpGetResult
{
    Poco::Net::HTTPResponse::HTTPStatus status;
    bool has_location;
    std::string location;
    std::string body;
};

HttpGetResult httpGet(uint16_t port, const std::string & path_and_query)
{
    Poco::Net::HTTPClientSession session("127.0.0.1", port);
    session.setTimeout(Poco::Timespan(10, 0));
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, path_and_query);
    session.sendRequest(request);

    Poco::Net::HTTPResponse response;
    std::istream & rs = session.receiveResponse(response);

    HttpGetResult result;
    result.status = response.getStatus();
    result.has_location = response.has("Location");
    result.location = response.get("Location", "");
    Poco::StreamCopier::copyToString(rs, result.body);
    return result;
}

} // anonymous namespace

TEST(OAuthLogin, LoopbackStartEndpointDoesNotLeakState)
{
    RunningCallbackServer srv;

    // `/start` (the removed leak vector) redirected to the auth URL, so a
    // reintroduced leak would surface as a 3xx whose Location header carries
    // `state` / `code_challenge`. Assert there is no redirect and — checking
    // the Location value itself, which is where such a leak would live — that
    // it discloses no secret.
    const HttpGetResult r = httpGet(srv.port, "/start");
    EXPECT_EQ(r.status, Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    EXPECT_FALSE(r.has_location);
    EXPECT_EQ(r.location.find("state="), std::string::npos);
    EXPECT_EQ(r.location.find("code_challenge="), std::string::npos);

    // Probing `/start` must never unblock the waiting flow.
    std::lock_guard<std::mutex> lock(srv.state.mtx);
    EXPECT_FALSE(srv.state.done);
}

TEST(OAuthLogin, LoopbackUnknownPathDoesNotCompleteFlow)
{
    RunningCallbackServer srv;

    const HttpGetResult r = httpGet(srv.port, "/favicon.ico");
    EXPECT_EQ(r.status, Poco::Net::HTTPResponse::HTTP_NOT_FOUND);

    std::lock_guard<std::mutex> lock(srv.state.mtx);
    EXPECT_FALSE(srv.state.done);
}

TEST(OAuthLogin, LoopbackCallbackCompletesFlow)
{
    RunningCallbackServer srv("deadbeef");

    // The only endpoint that drives completion is `/callback`, and only when
    // the delivered `state` matches the one the flow generated.
    const HttpGetResult r = httpGet(srv.port, "/callback?code=abc123&state=deadbeef");
    EXPECT_EQ(r.status, Poco::Net::HTTPResponse::HTTP_OK);

    std::lock_guard<std::mutex> lock(srv.state.mtx);
    EXPECT_TRUE(srv.state.done);
    EXPECT_EQ(srv.state.code, "abc123");
    EXPECT_TRUE(srv.state.error.empty());
}

TEST(OAuthLogin, LoopbackErrorCallbackWithMatchingStateCompletesFlow)
{
    RunningCallbackServer srv("deadbeef");

    // A spec-compliant IdP echoes `state` on error redirects too; such a
    // denial must complete the flow so the caller can surface the real error
    // (rather than time out). The state-matching gate accepts it, records the
    // error, and unblocks.
    const HttpGetResult r = httpGet(srv.port, "/callback?error=access_denied&state=deadbeef");
    EXPECT_EQ(r.status, Poco::Net::HTTPResponse::HTTP_OK);

    std::lock_guard<std::mutex> lock(srv.state.mtx);
    EXPECT_TRUE(srv.state.done);
    EXPECT_TRUE(srv.state.code.empty());
    EXPECT_EQ(srv.state.error, "access_denied");
}

TEST(OAuthLogin, LoopbackCallbackWithMismatchedStateIsRejected)
{
    RunningCallbackServer srv("expected-state");

    // A callback whose `state` does not match must be rejected and must NOT
    // unblock the flow — otherwise any local process could abort a genuine
    // login (DoS) by racing in a bogus callback before the real IdP redirect.
    const HttpGetResult r = httpGet(srv.port, "/callback?code=attacker&state=wrong-state");
    EXPECT_EQ(r.status, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);

    std::lock_guard<std::mutex> lock(srv.state.mtx);
    EXPECT_FALSE(srv.state.done);
    EXPECT_TRUE(srv.state.code.empty());
    // The mismatch is recorded so a subsequent timeout can report a likely CSRF
    // failure rather than a bare timeout.
    EXPECT_TRUE(srv.state.saw_state_mismatch);
}

TEST(OAuthLogin, LoopbackFirstMatchingCallbackWins)
{
    RunningCallbackServer srv("s");

    // Once a valid callback is recorded, a later one (e.g. an attacker racing
    // the IdP after the real redirect already landed) must not overwrite it.
    const HttpGetResult first = httpGet(srv.port, "/callback?code=first&state=s");
    EXPECT_EQ(first.status, Poco::Net::HTTPResponse::HTTP_OK);
    const HttpGetResult second = httpGet(srv.port, "/callback?code=second&state=s");
    EXPECT_EQ(second.status, Poco::Net::HTTPResponse::HTTP_OK);

    std::lock_guard<std::mutex> lock(srv.state.mtx);
    EXPECT_TRUE(srv.state.done);
    EXPECT_EQ(srv.state.code, "first");
}

#endif // USE_JWT_CPP && USE_SSL
