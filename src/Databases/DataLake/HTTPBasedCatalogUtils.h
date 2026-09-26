#pragma once

#include <IO/HTTPCommon.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <functional>
#include <chrono>
#include <optional>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPResponse.h>

namespace DataLake
{

struct AccessToken
{
    std::string token;
    std::optional<std::chrono::system_clock::time_point> expires_at;

    bool isExpired() const
    {
        if (!expires_at.has_value())
            return false;
        return std::chrono::system_clock::now() >= expires_at.value();
    }
};

/// POSTs an OAuth token request. An empty `body` means the parameters are in the URL query.
/// The URL is never included in errors or logs: its query may carry `client_secret`.
AccessToken requestOAuthToken(const DB::ContextPtr & context, const Poco::URI & url, const std::string & body);

/// Calls `make_request(/* force_refresh = */ false)`. When the catalog rejects the request with
/// 401/403, the cached OAuth token is likely stale, so the request runs once more with
/// `force_refresh = true`, which makes the caller mint a fresh token first. Retries at most once.
/// `enable_refresh` is false for catalogs whose credentials cannot be renewed, e.g. a static token.
template <typename Func>
auto requestWithTokenRefresh(bool enable_refresh, Func && make_request)
{
    if (!enable_refresh)
        return make_request(/* force_refresh = */ false);

    try
    {
        return make_request(/* force_refresh = */ false);
    }
    catch (const DB::HTTPException & e)
    {
        const auto status = e.getHTTPStatus();
        if (status != Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED
            && status != Poco::Net::HTTPResponse::HTTPStatus::HTTP_FORBIDDEN)
            throw;
        return make_request(/* force_refresh = */ true);
    }
}

DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
    const std::string & endpoint,
    DB::ContextPtr context,
    const std::string & bearer_token,
    const Poco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
    std::function<void(DB::WriteBuffer &)> out_stream_callaback = {});

std::pair<Poco::Dynamic::Var, std::string> makeHTTPRequestAndReadJSON(
    const std::string & endpoint,
    DB::ContextPtr context,
    const std::string & bearer_token,
    const Poco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
    std::function<void(DB::WriteBuffer &)> out_stream_callaback = {});

/// Validate a bearer token as the `Authorization: Bearer <token>` header that
/// `createWithBearerToken` synthesizes, applying the same `http_forbid_headers` and
/// control-character checks as a user-supplied `auth_header`. Throws BAD_ARGUMENTS on a forbidden
/// or malformed token; an empty token is a no-op (no header is sent).
void validateBearerToken(const DB::ContextPtr & context, const std::string & bearer_token);

}
