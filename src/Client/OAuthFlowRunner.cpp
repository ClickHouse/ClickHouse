#include <config.h>
#include <Client/OAuthFlowRunner.h>

#if USE_JWT_CPP && USE_SSL

#include <Client/OAuthProviderPolicy.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>

#include <Poco/AutoPtr.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Timespan.h>
#include <Poco/URI.h>

#include <openssl/rand.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#if defined(__APPLE__) || defined(__linux__)
#    include <spawn.h>
#    include <sys/wait.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int AUTHENTICATION_FAILED;
}

void writeCachedRefreshToken(const std::string & client_id, const std::string & refresh_token);

namespace
{

constexpr int HTTP_TIMEOUT_SECONDS = 30;

/// Hard cap on the size of an OAuth2 token / device-authorization response
/// body. Real responses are a few hundred bytes; we accept up to 1 MiB so a
/// hostile or compromised endpoint cannot stream gigabytes into a std::string
/// (memory-exhaustion DoS of clickhouse-client). Anything larger is treated
/// as a protocol error.
constexpr size_t OAUTH_MAX_RESPONSE_BYTES = 1 * 1024 * 1024;

/// Bounds for RFC 8628 device-flow timing values. The client treats the device
/// authorization endpoint as untrusted: a hostile or misconfigured server must
/// not be able to push the client into a tight poll loop (interval <= 0), an
/// uninterruptible multi-hour sleep (interval huge), or an effectively
/// unbounded polling window (expires_in huge). Out-of-range values are treated
/// as a protocol error; in-range values are additionally clamped so that a
/// single sleep_for() never exceeds DEVICE_FLOW_MAX_INTERVAL_SECONDS, which
/// also caps how long Ctrl-C remains unresponsive.
constexpr int DEVICE_FLOW_MIN_INTERVAL_SECONDS = 1;
constexpr int DEVICE_FLOW_MAX_INTERVAL_SECONDS = 60;
constexpr int DEVICE_FLOW_INTERVAL_HARD_LIMIT_SECONDS = 3600;
constexpr int DEVICE_FLOW_DEFAULT_INTERVAL_SECONDS = 5;

constexpr int DEVICE_FLOW_MIN_EXPIRES_IN_SECONDS = 60;
constexpr int DEVICE_FLOW_MAX_EXPIRES_IN_SECONDS = 1800;
constexpr int DEVICE_FLOW_EXPIRES_IN_HARD_LIMIT_SECONDS = 86400;
constexpr int DEVICE_FLOW_DEFAULT_EXPIRES_IN_SECONDS = 300;

constexpr int DEVICE_FLOW_SLOW_DOWN_INCREMENT_SECONDS = 5;

/// Cadence at which the device-flow polling loop emits a heartbeat to stderr.
/// Without this the client prints the URL/user_code once and then stays
/// completely silent (sometimes for the full 1800s expires_in clamp) while it
/// internally polls the token endpoint, leaving the user unable to tell
/// "still waiting for me to approve in the browser" apart from "the process
/// is wedged on a network call". 30s is short enough to give a perceptible
/// pulse for the default 300s window, and long enough that a 1800s window
/// produces ~60 lines of scrollback, not 360. The cadence is real-time, not
/// per-poll, so a server-driven slow_down ratchet doesn't silently stretch
/// the perceived gap between updates.
constexpr int DEVICE_FLOW_STATUS_INTERVAL_SECONDS = 30;

int extractDeviceFlowInt(const Poco::JSON::Object::Ptr & resp, const std::string & key, int default_value)
{
    if (!resp->has(key))
        return default_value;
    try
    {
        return resp->getValue<int>(key);
    }
    catch (const Poco::Exception &)
    {
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Device authorization response value '{}' is not a valid integer",
            key);
    }
}

/// Read up to max_bytes from `in` into `out`. Throws AUTHENTICATION_FAILED if
/// the stream contains more than max_bytes. Used to bound response sizes from
/// untrusted OAuth endpoints.
void copyStreamWithLimit(std::istream & in, std::string & out, size_t max_bytes)
{
    constexpr size_t buf_size = 8192;
    char buffer[buf_size];
    out.clear();
    while (in)
    {
        in.read(buffer, static_cast<std::streamsize>(buf_size));
        const auto got = static_cast<size_t>(in.gcount());
        if (got == 0)
            break;
        if (out.size() + got > max_bytes)
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED,
                "OAuth2 endpoint response exceeds size limit of {} bytes",
                max_bytes);
        out.append(buffer, got);
    }
}

std::string htmlEscape(const std::string & s)
{
    std::string out;
    out.reserve(s.size());
    for (char c : s)
    {
        switch (c)
        {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\'': out += "&#39;"; break;
            default: out += c; break;
        }
    }
    return out;
}

struct PKCEPair
{
    std::string verifier;
    std::string challenge;
};

PKCEPair generatePKCE()
{
    unsigned char raw[32];
    if (RAND_bytes(raw, sizeof(raw)) != 1)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "RAND_bytes failed for PKCE verifier");

    std::string verifier = base64Encode(
        std::string(reinterpret_cast<char *>(raw), sizeof(raw)),
        /*url_encoding=*/true,
        /*no_padding=*/true);

    std::string sha = encodeSHA256(verifier);
    std::string challenge = base64Encode(sha, /*url_encoding=*/true, /*no_padding=*/true);
    return {verifier, challenge};
}

void openBrowser(const std::string & url)
{
    std::cerr << "Opening browser for authentication.\n"
              << "If the browser does not open, visit:\n  " << url << "\n";

#if defined(__APPLE__) || defined(__linux__)
    const char * cmd =
#    if defined(__APPLE__)
        "open";
#    else
        "xdg-open";
#    endif
    const char * argv[] = {cmd, url.c_str(), nullptr};
    pid_t pid;
    /// posix_spawnp returns the error number directly (not via errno); a
    /// nonzero return means we never got to exec the helper at all (e.g.
    /// xdg-open is not installed on a headless host). A zero return followed
    /// by a nonzero waitpid exit status means the helper ran but failed to
    /// launch a browser (xdg-open exits 3 when no handler is registered).
    /// Without the diagnostic below, the caller would silently block in the
    /// 120s callback wait, which is the L2 hazard.
    if (posix_spawnp(&pid, cmd, nullptr, nullptr, const_cast<char * const *>(argv), nullptr) != 0)
    {
        std::cerr << "Unable to launch '" << cmd << "'; please copy the URL above into a browser manually.\n";
        return;
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
        std::cerr << "Unable to launch a browser via '" << cmd
                  << "'; please copy the URL above into a browser manually.\n";
#else
    std::cerr << "Automatic browser launch is not supported on this platform; "
                 "please copy the URL above into a browser manually.\n";
#endif
}

struct AuthCodeState
{
    std::mutex mtx;
    std::condition_variable cv;
    std::string code;
    std::string error;
    std::string received_state;
    bool done = false;

    /// Pre-loaded before the server starts so that the loopback server can
    /// serve the auth URL via a 302 redirect on /start. The browser helper is
    /// then launched with only the loopback URL on its argv, keeping the CSRF
    /// state and PKCE challenge out of /proc/<pid>/cmdline of the helper.
    /// Read-only after server.start(); never mutated by the handler.
    std::string auth_url;
};

class AuthCodeHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit AuthCodeHandler(AuthCodeState & state_) : state(state_) { }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        Poco::URI uri("http://localhost" + request.getURI());

        /// RFC 8252 §7.3: native-app loopback redirects must be accepted only
        /// at the registered redirect URI, and the OAuth2 redirect is always a
        /// GET. Any other method or path is either a stray request from the
        /// browser (e.g. /favicon.ico) or a local attacker probing the
        /// ephemeral port; in both cases we must respond with an error and
        /// must not unblock the main thread, otherwise the legitimate IdP
        /// redirect can be pre-empted (causing either a DoS of the flow or,
        /// if the attacker has obtained the CSRF state via /proc/<pid>/cmdline
        /// of the spawned browser helper, a code-injection race).
        if (request.getMethod() != Poco::Net::HTTPRequest::HTTP_GET)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED);
            response.setContentType("text/plain");
            response.send() << "Method Not Allowed";
            return;
        }

        const std::string & path = uri.getPath();

        /// The browser helper is launched against /start instead of the full
        /// auth URL so the CSRF state and PKCE challenge do not appear in any
        /// process argv. We hand the URL to the browser via a same-origin 302
        /// served by this loopback server. /start does not mutate auth state,
        /// so it is safe to serve it more than once.
        if (path == "/start")
        {
            std::string target;
            {
                std::lock_guard<std::mutex> lock(state.mtx);
                target = state.auth_url;
            }
            if (target.empty())
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                response.setContentType("text/plain");
                response.send() << "Not Found";
                return;
            }
            response.redirect(target, Poco::Net::HTTPResponse::HTTP_FOUND);
            return;
        }

        if (path != "/callback")
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
            response.setContentType("text/plain");
            response.send() << "Not Found";
            return;
        }

        const auto params = uri.getQueryParameters();

        std::string code;
        std::string error;
        std::string received_state;
        for (const auto & [k, v] : params)
        {
            if (k == "code")
                code = v;
            else if (k == "error")
                error = v;
            else if (k == "state")
                received_state = v;
        }

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentType("text/html");
        auto & out = response.send();
        if (!code.empty())
            out << "<html><body>Authentication successful. You may close this tab.</body></html>";
        else
            out << "<html><body>Authentication failed: " << htmlEscape(error) << "</body></html>";
        out.flush();

        std::lock_guard<std::mutex> lock(state.mtx);
        /// Only the first valid /callback delivery wins; subsequent requests
        /// (e.g. an attacker racing the IdP after the legitimate redirect has
        /// already been recorded) are ignored so they cannot overwrite a
        /// previously-validated code/state pair.
        if (state.done)
            return;
        state.code = code;
        state.error = error;
        state.received_state = received_state;
        state.done = true;
        state.cv.notify_one();
    }

private:
    AuthCodeState & state;
};

class AuthCodeHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit AuthCodeHandlerFactory(AuthCodeState & state_) : state(state_) { }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new AuthCodeHandler(state);
    }

private:
    AuthCodeState & state;
};

}

std::string urlEncodeOAuth(const std::string & value)
{
    std::string result;
    Poco::URI::encode(value, "", result);
    return result;
}

Poco::JSON::Object::Ptr postOAuthForm(const std::string & url, const std::string & body)
{
    Poco::URI uri(url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    request.setContentType("application/x-www-form-urlencoded");
    request.setContentLength(static_cast<std::streamsize>(body.size()));

    Poco::Net::HTTPResponse response;
    std::string response_body;

    if (uri.getScheme() == "https")
    {
        Poco::Net::Context::Ptr ctx = Poco::Net::SSLManager::instance().defaultClientContext();
        Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort(), ctx);
        session.setTimeout(Poco::Timespan(HTTP_TIMEOUT_SECONDS, 0));
        auto & req_stream = session.sendRequest(request);
        req_stream << body;
        auto & resp_stream = session.receiveResponse(response);
        copyStreamWithLimit(resp_stream, response_body, OAUTH_MAX_RESPONSE_BYTES);
    }
    else
    {
        Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
        session.setTimeout(Poco::Timespan(HTTP_TIMEOUT_SECONDS, 0));
        auto & req_stream = session.sendRequest(request);
        req_stream << body;
        auto & resp_stream = session.receiveResponse(response);
        copyStreamWithLimit(resp_stream, response_body, OAUTH_MAX_RESPONSE_BYTES);
    }

    Poco::Dynamic::Var parsed;
    try
    {
        Poco::JSON::Parser parser;
        parsed = parser.parse(response_body);
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "OAuth2 endpoint '{}' returned HTTP {} with non-JSON body: {}",
            url,
            static_cast<int>(response.getStatus()),
            response_body.substr(0, 512));
    }

    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "OAuth2 endpoint '{}' returned HTTP {} with non-object JSON response: {}",
            url,
            static_cast<int>(response.getStatus()),
            response_body.substr(0, 512));
    return obj;
}

std::string runOAuthAuthCodeFlow(const OAuthCredentials & creds)
{
    auto provider_policy = IOAuthProviderPolicy::create(creds);
    auto pkce = generatePKCE();

    unsigned char state_bytes[16];
    if (RAND_bytes(state_bytes, sizeof(state_bytes)) != 1)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "RAND_bytes failed for OAuth state");

    std::string csrf_state;
    csrf_state.reserve(32);
    for (unsigned char b : state_bytes)
    {
        constexpr char digits[] = "0123456789abcdef";
        csrf_state += digits[(b >> 4) & 0xF];
        csrf_state += digits[b & 0xF];
    }

    Poco::Net::ServerSocket server_socket;
    server_socket.bind(Poco::Net::SocketAddress("127.0.0.1", 0), /*reuse_address=*/true);
    server_socket.listen(1);
    const uint16_t port = server_socket.address().port();

    /// RFC 8252 §7.3 recommends the loopback IP literal over the hostname
    /// "localhost" for native-app redirect URIs. We bind only to 127.0.0.1, but
    /// "localhost" can resolve to ::1 first on dual-stack hosts (RFC 6724
    /// default ordering, /etc/hosts, or NSS/AAAA preference): in that case the
    /// browser's GET on the redirect lands on the IPv6 loopback where nothing
    /// is listening, the auth code is silently dropped, and the main thread
    /// hits the 120s wait_for() timeout even though the user successfully
    /// completed the login. Using the IP literal keeps the redirect target
    /// aligned with the bound socket regardless of resolver behaviour, and
    /// matches the host already used for the /start browser entry URL below.
    const std::string redirect_uri = "http://127.0.0.1:" + std::to_string(port) + "/callback";

    std::string auth_url
        = creds.auth_uri
        + "?response_type=code"
          "&client_id=" + urlEncodeOAuth(creds.client_id)
        + "&redirect_uri=" + urlEncodeOAuth(redirect_uri)
        + "&code_challenge=" + pkce.challenge
        + "&code_challenge_method=S256"
        + "&scope=" + urlEncodeOAuth(provider_policy->getAuthCodeScope())
        + "&state=" + csrf_state;
    if (provider_policy->useAccessTypeOfflineForAuthCode())
        auth_url += "&access_type=offline";

    AuthCodeState state;
    /// Publish the auth URL to the loopback server before it starts so /start
    /// can immediately redirect to it. This is set under the mutex for
    /// happens-before with the handler thread; in practice it is only ever
    /// read after server.start() but we keep the synchronization explicit.
    {
        std::lock_guard<std::mutex> lock(state.mtx);
        state.auth_url = auth_url;
    }
    auto params = Poco::AutoPtr<Poco::Net::HTTPServerParams>(new Poco::Net::HTTPServerParams());
    params->setMaxQueued(1);
    params->setMaxThreads(1);
    Poco::Net::HTTPServer server(new AuthCodeHandlerFactory(state), server_socket, params);
    server.start();

    /// Launch the browser against the loopback /start endpoint instead of the
    /// real auth URL. The full auth URL (including CSRF state and PKCE
    /// challenge) therefore never appears in any process's argv, closing the
    /// /proc/<pid>/cmdline disclosure path that local same-UID attackers
    /// previously had against the spawned xdg-open / open helper.
    const std::string browser_entry_url = "http://127.0.0.1:" + std::to_string(port) + "/start";
    openBrowser(browser_entry_url);

    bool timed_out = false;
    std::string received_code;
    std::string received_error;
    std::string received_state;
    {
        std::unique_lock<std::mutex> lock(state.mtx);
        timed_out = !state.cv.wait_for(lock, std::chrono::seconds(120), [&] { return state.done; });
        received_code = state.code;
        received_error = state.error;
        received_state = state.received_state;
    }
    server.stop();

    if (timed_out)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "OAuth2 login timed out waiting for browser callback");
    if (!received_error.empty())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "OAuth2 authorization error: {}", received_error);
    if (received_code.empty())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "OAuth2 callback did not contain an authorization code");
    if (received_state != csrf_state)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "OAuth2 CSRF check failed: unexpected state in callback");

    std::string body
        = "grant_type=authorization_code"
          "&code=" + urlEncodeOAuth(received_code)
        + "&redirect_uri=" + urlEncodeOAuth(redirect_uri)
        + "&client_id=" + urlEncodeOAuth(creds.client_id)
        + "&code_verifier=" + urlEncodeOAuth(pkce.verifier);
    /// Confidential clients append the registered secret; public clients
    /// (PKCE-only) must omit the parameter entirely. An empty value is not
    /// equivalent to omission and is rejected by several IdPs as invalid_client.
    if (!creds.client_secret.empty())
        body += "&client_secret=" + urlEncodeOAuth(creds.client_secret);

    auto resp = postOAuthForm(creds.token_uri, body);
    if (resp->has("error"))
    {
        const std::string desc = resp->has("error_description")
            ? resp->getValue<std::string>("error_description")
            : resp->getValue<std::string>("error");
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "OAuth2 token exchange failed: {}", desc);
    }

    if (!resp->has("id_token"))
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "OAuth2 token response did not contain id_token");

    if (resp->has("refresh_token"))
        writeCachedRefreshToken(creds.client_id, resp->getValue<std::string>("refresh_token"));

    return resp->getValue<std::string>("id_token");
}

std::string runOAuthDeviceFlow(OAuthCredentials creds)
{
    auto provider_policy = IOAuthProviderPolicy::create(creds);
    if (creds.device_auth_uri.empty())
        creds.device_auth_uri = provider_policy->resolveDeviceAuthorizationEndpoint(creds);

    const std::string device_scope = provider_policy->getDeviceScope();
    const std::string device_body
        = "client_id=" + urlEncodeOAuth(creds.client_id)
        + "&scope=" + urlEncodeOAuth(device_scope);

    auto device_resp = postOAuthForm(creds.device_auth_uri, device_body);

    if (device_resp->has("error"))
    {
        const std::string desc = device_resp->has("error_description")
            ? device_resp->getValue<std::string>("error_description")
            : device_resp->getValue<std::string>("error");
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Device authorization request failed: {}", desc);
    }

    if (!device_resp->has("device_code") || !device_resp->has("user_code"))
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Device authorization response from '{}' is missing required fields "
            "(device_code / user_code). Response: {}",
            creds.device_auth_uri,
            [&]
            {
                std::ostringstream ss;
                device_resp->stringify(ss);
                return ss.str();
            }());

    const std::string device_code = device_resp->getValue<std::string>("device_code");
    const std::string user_code = device_resp->getValue<std::string>("user_code");
    const std::string verification_uri = device_resp->has("verification_uri_complete")
        ? device_resp->getValue<std::string>("verification_uri_complete")
        : device_resp->has("verification_uri")
            ? device_resp->getValue<std::string>("verification_uri")
            : device_resp->has("verification_url")
                ? device_resp->getValue<std::string>("verification_url")
                : throw Exception(
                    ErrorCodes::AUTHENTICATION_FAILED,
                    "Device authorization response from '{}' is missing verification_uri / "
                    "verification_uri_complete / verification_url. Response: {}",
                    creds.device_auth_uri,
                    [&]
                    {
                        std::ostringstream ss;
                        device_resp->stringify(ss);
                        return ss.str();
                    }());

    int interval = extractDeviceFlowInt(device_resp, "interval", DEVICE_FLOW_DEFAULT_INTERVAL_SECONDS);
    int expires_in = extractDeviceFlowInt(device_resp, "expires_in", DEVICE_FLOW_DEFAULT_EXPIRES_IN_SECONDS);

    /// Reject values that are non-positive or wildly out of spec: a hostile or
    /// misconfigured device endpoint must not be able to coerce the client
    /// into a tight poll loop, a multi-hour uninterruptible sleep, or an
    /// effectively unbounded polling window.
    if (interval <= 0 || interval > DEVICE_FLOW_INTERVAL_HARD_LIMIT_SECONDS)
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Device authorization response specified an out-of-range polling interval: {} seconds",
            interval);
    if (expires_in <= 0 || expires_in > DEVICE_FLOW_EXPIRES_IN_HARD_LIMIT_SECONDS)
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Device authorization response specified an out-of-range expires_in: {} seconds",
            expires_in);

    /// Clamp into a sensible operational window. The interval upper bound also
    /// bounds how long a single sleep_for() blocks, which is the time window
    /// during which Ctrl-C cannot interrupt the flow.
    interval = std::clamp(interval, DEVICE_FLOW_MIN_INTERVAL_SECONDS, DEVICE_FLOW_MAX_INTERVAL_SECONDS);
    expires_in = std::clamp(expires_in, DEVICE_FLOW_MIN_EXPIRES_IN_SECONDS, DEVICE_FLOW_MAX_EXPIRES_IN_SECONDS);

    std::cerr << "\nTo authenticate, visit:\n  " << verification_uri << "\nAnd enter code: " << user_code << "\n\n";
    std::cerr << "Waiting for authorization (this code expires in " << expires_in << " seconds)...\n";

    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(expires_in);
    /// Real-time gate for the heartbeat below. Initialised to `start` rather
    /// than to "after the first poll" so the first heartbeat fires ~30s after
    /// the URL/code line, regardless of how long the first network round-trip
    /// takes.
    auto last_status = start;
    while (std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::seconds(interval));

        std::string poll_body
            = "grant_type=urn:ietf:params:oauth:grant-type:device_code"
              "&device_code=" + urlEncodeOAuth(device_code)
            + "&client_id=" + urlEncodeOAuth(creds.client_id);
        /// See runOAuthAuthCodeFlow() above: omit, do not send empty.
        if (!creds.client_secret.empty())
            poll_body += "&client_secret=" + urlEncodeOAuth(creds.client_secret);

        auto resp = postOAuthForm(creds.token_uri, poll_body);
        if (resp->has("error"))
        {
            const std::string err = resp->getValue<std::string>("error");
            if (err == "authorization_pending" || err == "slow_down")
            {
                if (err == "slow_down")
                {
                    /// Per RFC 8628 the client must increase its polling
                    /// interval, but the new value still has to stay inside
                    /// our operational bound so a server cannot ratchet the
                    /// interval up indefinitely. Surface the change as a
                    /// one-shot line — slow_down is rare in practice, and a
                    /// silent ratchet would be confusing if the user is
                    /// timing the flow against the deadline they were just
                    /// shown. Reset last_status so the next heartbeat doesn't
                    /// fire immediately afterwards.
                    interval = std::min(
                        interval + DEVICE_FLOW_SLOW_DOWN_INCREMENT_SECONDS,
                        DEVICE_FLOW_MAX_INTERVAL_SECONDS);
                    std::cerr << "Server requested slower polling; new interval is " << interval << "s.\n";
                    last_status = std::chrono::steady_clock::now();
                }

                /// Heartbeat: keep the user oriented during the (potentially
                /// very long) wait between issuing the user_code and the user
                /// completing approval in their browser. Gated on real time
                /// rather than poll count so the cadence is stable across
                /// interval changes (default, slow_down, clamping).
                const auto now = std::chrono::steady_clock::now();
                if (now - last_status >= std::chrono::seconds(DEVICE_FLOW_STATUS_INTERVAL_SECONDS))
                {
                    const auto remaining = std::chrono::duration_cast<std::chrono::seconds>(deadline - now).count();
                    std::cerr << "Still waiting for authorization... (" << remaining << "s remaining)\n";
                    last_status = now;
                }
                continue;
            }
            const std::string desc = resp->has("error_description") ? resp->getValue<std::string>("error_description") : err;
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Device flow error: {}", desc);
        }

        if (!resp->has("id_token"))
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Device flow token response did not contain id_token");

        if (resp->has("refresh_token"))
            writeCachedRefreshToken(creds.client_id, resp->getValue<std::string>("refresh_token"));

        std::cerr << "Authentication successful.\n";
        return resp->getValue<std::string>("id_token");
    }

    throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Device flow timed out");
}

} // namespace DB

#endif // USE_JWT_CPP && USE_SSL
