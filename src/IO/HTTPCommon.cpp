#include <IO/HTTPCommon.h>

#include <Server/HTTP/HTTPServerResponse.h>
#include <Poco/Any.h>
#include <Common/Exception.h>

#include "config.h"

#if USE_SSL
#    include <Poco/Net/AcceptCertificateHandler.h>
#    include <Poco/Net/Context.h>
#    include <Poco/Net/HTTPSClientSession.h>
#    include <Poco/Net/InvalidCertificateHandler.h>
#    include <Poco/Net/PrivateKeyPassphraseHandler.h>
#    include <Poco/Net/RejectCertificateHandler.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

#include <Poco/Util/Application.h>

#include <istream>
#include <sstream>
#include <unordered_map>
#include <Common/ProxyConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
}

void setResponseDefaultHeaders(HTTPServerResponse & response)
{
    if (!response.getKeepAlive())
        return;

    const size_t keep_alive_timeout = response.getSession().getKeepAliveTimeout();
    const size_t keep_alive_max_requests = response.getSession().getMaxKeepAliveRequests();
    if (keep_alive_timeout)
    {
        if (keep_alive_max_requests)
            response.set("Keep-Alive", fmt::format("timeout={}, max={}", keep_alive_timeout, keep_alive_max_requests));
        else
            response.set("Keep-Alive", fmt::format("timeout={}", keep_alive_timeout));
    }
}

HTTPSessionPtr makeHTTPSession(
    HTTPConnectionGroupType group,
    const Poco::URI & uri,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_configuration)
{
    auto connection_pool = HTTPConnectionPools::instance().getPool(group, uri, proxy_configuration);
    return connection_pool->getConnection(timeouts);
}

bool isRedirect(const Poco::Net::HTTPResponse::HTTPStatus status) { return status == Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY  || status == Poco::Net::HTTPResponse::HTTP_FOUND || status == Poco::Net::HTTPResponse::HTTP_SEE_OTHER  || status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT; }

std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, const bool allow_redirects)
{
    auto & istr = session.receiveResponse(response);
    assertResponseIsOk(request.getURI(), response, istr, allow_redirects);
    return &istr;
}

void assertResponseIsOk(const String & uri, Poco::Net::HTTPResponse & response, std::istream & istr, const bool allow_redirects)
{
    auto status = response.getStatus();

    if (!(status == Poco::Net::HTTPResponse::HTTP_OK
        || status == Poco::Net::HTTPResponse::HTTP_CREATED
        || status == Poco::Net::HTTPResponse::HTTP_ACCEPTED
        || status == Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT /// Reading with Range header was successful.
        || (isRedirect(status) && allow_redirects)))
    {
        int code = status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS
            ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
            : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;

        istr.seekg(0, std::ios::end);
        size_t body_length = istr.tellg();
        throw HTTPException(code, uri, status, response.getReason(), body_length);
    }
}

Exception HTTPException::makeExceptionMessage(
    int code,
    const std::string & uri,
    Poco::Net::HTTPResponse::HTTPStatus http_status,
    const std::string & reason,
    size_t body_length)
{
    return Exception(code,
        "Received error from remote server {}. "
        "HTTP status code: {} '{}', "
        "body length: {} bytes",
        uri, static_cast<int>(http_status), reason, body_length);
}

}
