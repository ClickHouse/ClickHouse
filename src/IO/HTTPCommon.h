#pragma once

#include <memory>
#include <mutex>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/URIStreamFactory.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ProxyConfiguration.h>

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
        size_t body_length = 0
    )
        : Exception(makeExceptionMessage(code, uri, http_status_, reason, body_length))
        , http_status(http_status_)
    {}

    HTTPException * clone() const override { return new HTTPException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

    Poco::Net::HTTPResponse::HTTPStatus getHTTPStatus() const { return http_status; }

private:
    Poco::Net::HTTPResponse::HTTPStatus http_status{};

    static Exception makeExceptionMessage(
        int code,
        const std::string & uri,
        Poco::Net::HTTPResponse::HTTPStatus http_status,
        const std::string & reason,
        size_t body_length);

    const char * name() const noexcept override { return "DB::HTTPException"; }
    const char * className() const noexcept override { return "DB::HTTPException"; }
};

using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

void setResponseDefaultHeaders(HTTPServerResponse & response);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(
    HTTPConnectionGroupType group,
    const Poco::URI & uri,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_config = {}
);

bool isRedirect(Poco::Net::HTTPResponse::HTTPStatus status);

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, bool allow_redirects);

void assertResponseIsOk(
    const String & uri, Poco::Net::HTTPResponse & response, std::istream & istr, bool allow_redirects = false);

}
