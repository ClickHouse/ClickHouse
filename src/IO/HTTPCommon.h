#pragma once

#include <memory>
#include <mutex>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Common/PoolBase.h>
#include <Common/ProxyConfiguration.h>
#include <Poco/URIStreamFactory.h>

#include <IO/ConnectionTimeouts.h>


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
        const std::string & body
    )
        : Exception(makeExceptionMessage(code, uri, http_status_, reason, body))
        , http_status(http_status_)
    {}

    HTTPException * clone() const override { return new HTTPException(*this); }
    void rethrow() const override { throw *this; }

    int getHTTPStatus() const { return http_status; }

private:
    Poco::Net::HTTPResponse::HTTPStatus http_status{};

    static Exception makeExceptionMessage(
        int code,
        const std::string & uri,
        Poco::Net::HTTPResponse::HTTPStatus http_status,
        const std::string & reason,
        const std::string & body);

    const char * name() const noexcept override { return "DB::HTTPException"; }
    const char * className() const noexcept override { return "DB::HTTPException"; }
};

using PooledHTTPSessionPtr = PoolBase<Poco::Net::HTTPClientSession>::Entry; // SingleEndpointHTTPSessionPool::Entry
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

/// If a session have this tag attached, it will be reused without calling `reset()` on it.
/// All pooled sessions don't have this tag attached after being taken from a pool.
/// If the request and the response were fully written/read, the client code should add this tag
/// explicitly by calling `markSessionForReuse()`.
///
/// Note that HTTP response may contain extra bytes after the last byte of the payload. Specifically,
/// when chunked encoding is used, there's an empty chunk at the end. Those extra bytes must also be
/// read before the session can be reused. So we usually put an `istr->ignore(INT64_MAX)` call
/// before `markSessionForReuse()`.
struct HTTPSessionReuseTag
{
};

void markSessionForReuse(Poco::Net::HTTPSession & session);
void markSessionForReuse(HTTPSessionPtr session);
void markSessionForReuse(PooledHTTPSessionPtr session);


void setResponseDefaultHeaders(HTTPServerResponse & response, size_t keep_alive_timeout);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(
    const Poco::URI & uri,
    const ConnectionTimeouts & timeouts,
    ProxyConfiguration proxy_config = {}
);

/// As previous method creates session, but takes it from pool, without and with proxy uri.
///
/// The max_connections_per_endpoint parameter makes it look like the pool size can be different for
/// different requests (whatever that means), but actually we just assign the endpoint's connection
/// pool size when we see the endpoint for the first time, then we never change it.
/// We should probably change how this configuration works, and how this pooling works in general:
///  * Make the per_endpoint_pool_size be a global server setting instead of per-disk or per-query.
///  * Have boolean per-disk/per-query settings for enabling/disabling pooling.
///  * Add a limit on the number of endpoints and the total number of sessions across all endpoints.
///  * Enable pooling by default everywhere. In particular StorageURL and StorageS3.
///    (Enabling it for StorageURL is scary without the previous item - the user may query lots of
///     different endpoints. So currently pooling is mainly used for S3.)
PooledHTTPSessionPtr makePooledHTTPSession(
    const Poco::URI & uri,
    const ConnectionTimeouts & timeouts,
    size_t per_endpoint_pool_size,
    bool wait_on_pool_size_limit = true,
    ProxyConfiguration proxy_config = {});

bool isRedirect(Poco::Net::HTTPResponse::HTTPStatus status);

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, bool allow_redirects);

void assertResponseIsOk(
    const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, std::istream & istr, bool allow_redirects = false);

void setTimeouts(Poco::Net::HTTPClientSession & session, const ConnectionTimeouts & timeouts);
}
