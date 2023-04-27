#pragma once

#include <iostream>
#include <memory>
#include <mutex>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Common/PoolBase.h>
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

/// Pass this to HTTPClientSession's attachSessionData() to indicate that the session is in a clean
/// state and can be reused by a session pool. Otherwise the pool will discard or reset the session.
///
/// Do it after successfully exhausting the HTTP response istream. (Have to hit eof, even if there
/// are no content bytes left! Because with chunked encoding, there's an empty extra chunk at the end.)
/// Would probably be better for HTTPClientSession to keep track of this automatically.
///
/// Also consider the race condition inherent in HTTP keepalive: we may send a request at the same
/// time the server is closing the connection. In this case we get a network error and can't tell
/// whether the request has been processed by the server or not.
/// So we should only use session pooling if requests can be retried.
struct HTTPSessionReusableTag {};

using PooledHTTPSessionPtr = PoolBase<Poco::Net::HTTPClientSession>::Entry; // SingleEndpointHTTPSessionPool::Entry
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

void setResponseDefaultHeaders(HTTPServerResponse & response, size_t keep_alive_timeout);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, bool resolve_host = true);

/// As previous method creates session, but tooks it from pool, without and with proxy uri.
PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host = true);
PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const Poco::URI & proxy_uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host = true);

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
}
