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

class SingleEndpointHTTPSessionPool : public PoolBase<Poco::Net::HTTPClientSession>
{
private:
    const std::string host;
    const UInt16 port;
    const bool https;
    using Base = PoolBase<Poco::Net::HTTPClientSession>;

    ObjectPtr allocObject() override;

public:
    SingleEndpointHTTPSessionPool(const std::string & host_, UInt16 port_, bool https_, size_t max_pool_size_);
};

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

using PooledHTTPSessionPtr = SingleEndpointHTTPSessionPool::Entry;
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
