#include <IO/HTTPCommon.h>

#include <Server/HTTP/HTTPServerResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Common/Exception.h>
#include <Common/maskURIPassword.h>

#include <boost/algorithm/string/replace.hpp>

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


#include <algorithm>
#include <array>
#include <istream>
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
    const ProxyConfiguration & proxy_configuration,
    UInt64 * connect_time)
{
    auto connection_pool = HTTPConnectionPools::instance().getPool(group, uri, proxy_configuration);
    return connection_pool->getConnection(timeouts, connect_time);
}

bool isRedirect(const Poco::Net::HTTPResponse::HTTPStatus status) { return status == Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY  || status == Poco::Net::HTTPResponse::HTTP_FOUND || status == Poco::Net::HTTPResponse::HTTP_SEE_OTHER  || status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT; }

bool isRetriableHTTPError(const Poco::Net::HTTPResponse::HTTPStatus http_status) noexcept
{
    static constexpr std::array non_retriable_errors{
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_BAD_REQUEST,
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED,
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND,
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_FORBIDDEN,
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_IMPLEMENTED,
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_METHOD_NOT_ALLOWED,
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_CONFLICT};

    return std::all_of(
        non_retriable_errors.begin(), non_retriable_errors.end(), [&](const auto status) { return http_status != status; });
}

std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, const bool allow_redirects)
{
    auto & istr = session.receiveResponse(response);
    assertResponseIsOk(request.getURI(), response, istr, allow_redirects, requestCredentialSecrets(request));
    return &istr;
}

Strings requestCredentialSecrets(const Poco::Net::HTTPRequest & request)
{
    if (!request.has("Authorization"))
        return {};

    const std::string & authorization = request.get("Authorization");
    static constexpr std::string_view BEARER = "Bearer ";
    if (authorization.starts_with(BEARER))
        return {authorization.substr(BEARER.length())};

    static constexpr std::string_view BASIC = "Basic ";
    if (authorization.starts_with(BASIC))
    {
        /// `HTTPBasicCredentials` decodes the base64 user name and password from the header.
        Poco::Net::HTTPBasicCredentials credentials(request);
        Strings secrets;
        if (!credentials.getUsername().empty())
            secrets.push_back(credentials.getUsername());
        if (!credentials.getPassword().empty())
            secrets.push_back(credentials.getPassword());
        return secrets;
    }

    return {};
}

void assertResponseIsOk(
    const String & uri, Poco::Net::HTTPResponse & response, std::istream & istr, const bool allow_redirects,
    const Strings & body_secrets)
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

        std::string body;
        Poco::StreamCopier::copyToString(istr, body);

        throw HTTPException(code, uri, status, response.getReason(), body, body_secrets);
    }
}

Exception HTTPException::makeExceptionMessage(
    int code,
    const std::string & uri,
    Poco::Net::HTTPResponse::HTTPStatus http_status,
    const std::string & reason,
    const std::string & body,
    const Strings & body_secrets)
{
    std::string masked_uri = uri;
    maskURICredentials(masked_uri);

    /// The response body is remote content that can reflect the request back: its URL (masked here for
    /// presigned parameters) and the request's own credentials, which a server may echo verbatim (e.g.
    /// an auth error naming the user). Scrub those exact credential strings, keeping the rest of the
    /// body - a useful diagnostic that does not carry a credential.
    std::string masked_body = body;
    maskPresignedURLParameters(masked_body);
    for (const auto & secret : body_secrets)
        if (!secret.empty())
            boost::replace_all(masked_body, secret, "[HIDDEN]");

    return Exception(code,
        "Received error from remote server {}. "
        "HTTP status code: {} '{}', "
        "body length: {} bytes, body: '{}'",
        masked_uri, static_cast<int>(http_status), reason, body.length(), masked_body);
}

}
