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


namespace Poco
{
namespace Net
{
    class HTTPServerResponse;
}
}


namespace DB
{

constexpr int HTTP_TOO_MANY_REQUESTS = 429;

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

using PooledHTTPSessionPtr = SingleEndpointHTTPSessionPool::Entry;
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, unsigned keep_alive_timeout);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, bool resolve_host = true);

/// As previous method creates session, but tooks it from pool
PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size);

bool isRedirect(const Poco::Net::HTTPResponse::HTTPStatus status);

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, bool allow_redirects);

void assertResponseIsOk(
    const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, std::istream & istr, const bool allow_redirects = false);
}
