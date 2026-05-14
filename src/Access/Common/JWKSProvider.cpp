#include <Access/Common/JWKSProvider.h>

#if USE_JWT_CPP
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <system_error>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/StreamCopier.h>
#include <fstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int INVALID_CONFIG_PARAMETER;
}

JWKSType JWKSClient::getJWKS()
{
    /// `last_request_send` semantics: timestamp of the most recent fetch
    /// *attempt*, success or failure. Updated unconditionally before the
    /// HTTP call so a failed fetch doesn't leave the timestamp stale and
    /// invite every concurrent thread to re-hammer a failing endpoint
    /// (L-02). Within `refresh_timeout` of an attempt:
    ///   - if a previously-successful JWKS is cached, serve it.
    ///   - otherwise, throw a "fetch in cooldown" exception so callers
    ///     don't queue up new attempts during the back-off window.

    {
        std::shared_lock lock(mutex);
        auto now = std::chrono::steady_clock::now();
        if (last_request_send.has_value())
        {
            auto diff = std::chrono::duration<double>(now - *last_request_send).count();
            if (diff < static_cast<double>(refresh_timeout))
            {
                if (cached_jwks.has_value())
                    return cached_jwks.value();
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                                "JWKS endpoint at '{}' is in cooldown after a recent failed fetch; will retry after the cache lifetime elapses",
                                jwks_uri.toString());
            }
        }
    }

    std::unique_lock lock(mutex);
    auto now = std::chrono::steady_clock::now();
    if (last_request_send.has_value())
    {
        auto diff = std::chrono::duration<double>(now - *last_request_send).count();
        if (diff < static_cast<double>(refresh_timeout))
        {
            if (cached_jwks.has_value())
                return cached_jwks.value();
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "JWKS endpoint at '{}' is in cooldown after a recent failed fetch; will retry after the cache lifetime elapses",
                            jwks_uri.toString());
        }
    }

    /// Mark the attempt before issuing the network call so that even if the
    /// fetch throws, subsequent waiters on this mutex see an updated
    /// `last_request_send` and short-circuit via the cooldown branches above
    /// instead of repeating the failing fetch back-to-back.
    last_request_send = now;

    Poco::Net::HTTPResponse response;
    std::string response_string;

    Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, jwks_uri.getPathAndQuery()};

    /// Bound every JWKS fetch to a known limit. Without this, Poco's default
    /// `HTTPSession` timeout of 60 seconds applies, and because the JWKS fetch
    /// runs while `ExternalAuthenticators::mutex` is held by the outer
    /// `checkTokenCredentials` call, a single slow or hung JWKS endpoint would
    /// stall the whole auth subsystem (LDAP, Kerberos, HTTP basic, all other
    /// token auth paths) for up to a full minute per request. 10 seconds is a
    /// conservative cap: well above any healthy provider latency, well below
    /// the default.
    const Poco::Timespan jwks_http_timeout(/*seconds=*/10, 0);

    if (jwks_uri.getScheme() == "https")
    {
        Poco::Net::HTTPSClientSession session = Poco::Net::HTTPSClientSession(jwks_uri.getHost(), jwks_uri.getPort());
        session.setTimeout(jwks_http_timeout, jwks_http_timeout, jwks_http_timeout);
        session.sendRequest(request);
        std::istream & response_stream = session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK || !response_stream)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to get user info by access token, code: {}, reason: {}",
                response.getStatus(), response.getReason());
        Poco::StreamCopier::copyToString(response_stream, response_string);
    }
    else
    {
        Poco::Net::HTTPClientSession session = Poco::Net::HTTPClientSession(jwks_uri.getHost(), jwks_uri.getPort());
        session.setTimeout(jwks_http_timeout, jwks_http_timeout, jwks_http_timeout);
        session.sendRequest(request);
        std::istream & response_stream = session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK || !response_stream)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to get user info by access token, code: {}, reason: {}", response.getStatus(), response.getReason());
        Poco::StreamCopier::copyToString(response_stream, response_string);
    }

    JWKSType parsed_jwks;

    try
    {
        parsed_jwks = jwt::parse_jwks(response_string);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to parse JWKS: {}", e.what());
    }

    cached_jwks = std::move(parsed_jwks);
    return cached_jwks.value();
}

StaticJWKSParams::StaticJWKSParams(const std::string & static_jwks_, const std::string & static_jwks_file_)
{
    if (static_jwks_.empty() && static_jwks_file_.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "JWT validator misconfigured: `static_jwks` or `static_jwks_file` keys must be present in static JWKS validator configuration");
    if (!static_jwks_.empty() && !static_jwks_file_.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "JWT validator misconfigured: `static_jwks` and `static_jwks_file` keys cannot both be present in static JWKS validator configuration");

    static_jwks = static_jwks_;
    static_jwks_file = static_jwks_file_;
}

StaticJWKS::StaticJWKS(const StaticJWKSParams & params)
{
    static_jwks_file = params.static_jwks_file;

    String content = String(params.static_jwks);
    if (!static_jwks_file.empty())
    {
        std::ifstream ifs(static_jwks_file);
        Poco::StreamCopier::copyToString(ifs, content);
        /// Record the mtime so subsequent `getJWKS()` calls can notice rotation.
        std::error_code ec;
        const auto write_time = std::filesystem::last_write_time(static_jwks_file, ec);
        if (!ec)
            last_loaded_mtime = write_time;
    }
    try
    {
        auto keys = jwt::parse_jwks(content);
        jwks = std::move(keys);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to parse JWKS: {}", e.what());
    }
}

void StaticJWKS::reloadFromFileIfChangedNoLock()
{
    /// Inline `static_jwks` source: nothing to refresh from disk.
    if (static_jwks_file.empty())
        return;

    std::error_code ec;
    const auto mtime = std::filesystem::last_write_time(static_jwks_file, ec);
    if (ec)
    {
        /// File disappeared or became unreadable. Keep the previously-loaded
        /// keys -- failing closed here would lock everyone out on a transient
        /// filesystem hiccup. The operator gets a log signal.
        LOG_WARNING(getLogger("TokenAuthentication"),
                    "StaticJWKS: failed to stat '{}' for refresh ({}); keeping previously-loaded keys.",
                    static_jwks_file, ec.message());
        return;
    }
    if (mtime <= last_loaded_mtime)
        return;

    /// File has been rotated. Read + parse + swap.
    String content;
    try
    {
        std::ifstream ifs(static_jwks_file);
        Poco::StreamCopier::copyToString(ifs, content);
        auto new_keys = jwt::parse_jwks(content);
        jwks = std::move(new_keys);
        last_loaded_mtime = mtime;
        LOG_INFO(getLogger("TokenAuthentication"),
                 "StaticJWKS: reloaded keys from '{}' after detecting mtime change.", static_jwks_file);
    }
    catch (const std::exception & e)
    {
        /// Malformed new JWKS: keep the old one. Loud signal so the operator
        /// knows the rotation didn't take.
        LOG_ERROR(getLogger("TokenAuthentication"),
                  "StaticJWKS: failed to parse '{}' on refresh: {}; keeping previously-loaded keys.",
                  static_jwks_file, e.what());
    }
}

JWKSType StaticJWKS::getJWKS()
{
    /// Fast path: shared lock + mtime check. Refresh under exclusive lock only
    /// when the file actually changed.
    {
        std::shared_lock lock(mutex);
        if (static_jwks_file.empty())
            return jwks;

        std::error_code ec;
        const auto mtime = std::filesystem::last_write_time(static_jwks_file, ec);
        if (ec)
            return jwks;
        if (mtime <= last_loaded_mtime)
            return jwks;
    }

    std::unique_lock lock(mutex);
    reloadFromFileIfChangedNoLock();
    return jwks;
}

}
#endif
