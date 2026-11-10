#pragma once

#include <memory>
#include <mutex>
#include <string_view>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/URIStreamFactory.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ProxyConfiguration.h>
#include <Core/Types.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTP/HTTPClientIO.h>


namespace DB
{

class HTTPServerResponse;

class HTTPException : public Exception
{
public:
    HTTPException(
        int code,
        const std::string & uri,
        Poco::Net::HTTPResponse::HTTPStatus http_status_,
        const std::string & reason,
        const std::string & body,
        const Strings & response_secrets = {}
    )
        : Exception(makeExceptionMessage(code, uri, http_status_, reason, body, response_secrets))
        , http_status(http_status_)
        , response_body(body)
    {}

    HTTPException * clone() const override { return new HTTPException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)

    Poco::Net::HTTPResponse::HTTPStatus getHTTPStatus() const { return http_status; }
    /// Kept apart from the message so a caller can read the structured error a server put in it.
    const std::string & getResponseBody() const { return response_body; }

private:
    Poco::Net::HTTPResponse::HTTPStatus http_status{};
    std::string response_body;

    static Exception makeExceptionMessage(
        int code,
        const std::string & uri,
        Poco::Net::HTTPResponse::HTTPStatus http_status,
        const std::string & reason,
        const std::string & body,
        const Strings & response_secrets);

    const char * name() const noexcept override { return "DB::HTTPException"; }
    const char * className() const noexcept override { return "DB::HTTPException"; }
};

void setResponseDefaultHeaders(HTTPServerResponse & response);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(
    HTTPConnectionGroupType group,
    const Poco::URI & uri,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_config = {},
    UInt64 * connect_time = nullptr
);

bool isRedirect(Poco::Net::HTTPResponse::HTTPStatus status);

/// Whether an HTTP error response is worth retrying. Deterministic client errors (bad request,
/// unauthorized, forbidden, not found, method not allowed, not implemented) are not retriable;
/// everything else (transient/server-side errors, rate limiting, …) is.
bool isRetriableHTTPError(Poco::Net::HTTPResponse::HTTPStatus http_status) noexcept;

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * The returned buffer reads the body from 'session', which has to outlive it.
  */
std::unique_ptr<HTTPResponseReadBuffer> receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, bool allow_redirects);

/// Returns the Bearer token carried by a request's `Authorization` header, so a caller can scrub it
/// from an error response that reflects it back. A `Basic` header is not decoded, so a password
/// is not scrubbed. Empty when there is no bearer token.
Strings requestCredentialSecrets(const Poco::Net::HTTPRequest & request);

/// Replaces the credentials of a request, the presigned-URL ones carried by `uri` and `request_secrets`, with `[HIDDEN]`
/// in a text its server sent back, as `HTTPException` does with the reason phrase and the body.
std::string scrubRequestCredentials(std::string_view text, const std::string & uri, const Strings & request_secrets);

/// `response_secrets` are credential strings to scrub from the reflected reason phrase and body (see
/// `requestCredentialSecrets`): a remote error response can echo the request's own credentials.
void assertResponseIsOk(
    const String & uri, Poco::Net::HTTPResponse & response, ReadBuffer & body, bool allow_redirects = false,
    const Strings & response_secrets = {});

}
