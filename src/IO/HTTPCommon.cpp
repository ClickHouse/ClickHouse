#include <IO/HTTPCommon.h>

#include <IO/ReadHelpers.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Common/Exception.h>
#include <Common/HiddenSecret.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/maskURIPassword.h>

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

bool isRedirect(const Poco::Net::HTTPResponse::HTTPStatus status)
{
    return status == Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY
        || status == Poco::Net::HTTPResponse::HTTP_FOUND
        || status == Poco::Net::HTTPResponse::HTTP_SEE_OTHER
        || status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT
        /// `308 Permanent Redirect` (RFC 9110, 15.4.9) keeps the method and the body like `307`.
        || status == Poco::Net::HTTPResponse::HTTP_PERMANENT_REDIRECT;
}

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

std::unique_ptr<HTTPResponseReadBuffer> receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, const bool allow_redirects)
{
    auto body = receiveHTTPResponse(session, response);
    assertResponseIsOk(request.getURI(), response, *body, allow_redirects, requestCredentialSecrets(request));
    return body;
}

Strings requestCredentialSecrets(const Poco::Net::HTTPRequest & request)
{
    if (!request.has("Authorization"))
        return {};

    /// `getCredentials` splits the scheme off the header value the way an HTTP peer does: the scheme
    /// is matched case-insensitively, and a field value may begin with whitespace (RFC 9110, 5.5),
    /// which `auth_header = 'Authorization: Bearer <token>'` leaves in front of the scheme.
    std::string scheme;
    std::string auth_info;
    request.getCredentials(scheme, auth_info);
    Poco::trimInPlace(auth_info);
    /// Only a bearer token is recoverable from the header, and it is a secret as a whole. A `Basic`
    /// header holds base64 of `user:password` and is deliberately not decoded here: its value is
    /// user input whenever it came from `headers()`, decoding throws on anything outside the base64
    /// alphabet, and the user name of a pair is an identifier rather than a credential. A password
    /// is therefore not scrubbed from a response body; no peer is known to echo one back.
    if (Poco::icompare(scheme, "Bearer") == 0 && !auth_info.empty())
        return {auth_info};

    return {};
}

void assertResponseIsOk(
    const String & uri, Poco::Net::HTTPResponse & response, ReadBuffer & body, const bool allow_redirects,
    const Strings & response_secrets)
{
    auto status = response.getStatus();

    if (!(status == Poco::Net::HTTPResponse::HTTP_OK
        || status == Poco::Net::HTTPResponse::HTTP_CREATED
        || status == Poco::Net::HTTPResponse::HTTP_ACCEPTED
        || status == Poco::Net::HTTPResponse::HTTP_NO_CONTENT /// Success without a body, e.g. Iceberg REST DropTable.
        || status == Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT /// Reading with Range header was successful.
        || (isRedirect(status) && allow_redirects)))
    {
        int code = status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS
            ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
            : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;

        std::string body_text;
        readStringUntilEOF(body_text, body);

        throw HTTPException(code, uri, status, response.getReason(), body_text, response_secrets);
    }
}

namespace
{

/// Replaces each run of characters covered by a secret with one `[HIDDEN]`. Matches are found in the original text only,
/// so a marker is never searched again and the output grows at most linearly.
std::string scrubSecrets(std::string_view text, const Strings & secrets)
{
    VectorWithMemoryTracking<bool> hidden(text.size());
    for (const auto & secret : secrets)
    {
        if (secret.empty())
            continue;
        for (size_t pos = text.find(secret); pos != std::string_view::npos; pos = text.find(secret, pos + secret.size()))
            std::fill_n(hidden.begin() + pos, secret.size(), true);
    }

    std::string result;
    result.reserve(text.size());
    for (size_t i = 0; i < text.size(); ++i)
    {
        if (!hidden[i])
            result += text[i];
        else if (i == 0 || !hidden[i - 1])
            result += HIDDEN_SECRET;
    }
    return result;
}

Strings collectRequestCredentials(const std::string & uri, const Strings & request_secrets)
{
    Strings secrets = presignedURLCredentials(uri);
    secrets.insert(secrets.end(), request_secrets.begin(), request_secrets.end());
    std::ranges::sort(secrets);
    secrets.erase(std::ranges::unique(secrets).begin(), secrets.end());
    return secrets;
}

}

std::string scrubRequestCredentials(std::string_view text, const std::string & uri, const Strings & request_secrets)
{
    return scrubSecrets(text, collectRequestCredentials(uri, request_secrets));
}

Exception HTTPException::makeExceptionMessage(
    int code,
    const std::string & uri,
    Poco::Net::HTTPResponse::HTTPStatus http_status,
    const std::string & reason,
    const std::string & body,
    const Strings & response_secrets)
{
    std::string masked_uri = uri;
    maskURICredentials(masked_uri);

    /// The reason phrase and the body are remote content that can reflect the request's own credentials back (e.g. an
    /// auth error naming the token). Scrub those exact credential strings, keeping the rest - a useful diagnostic that
    /// does not carry a credential. `uri` arrives unmasked, so that the credentials it carries are scrubbed here too.
    const Strings secrets = collectRequestCredentials(uri, response_secrets);

    return Exception(code,
        "Received error from remote server {}. "
        "HTTP status code: {} '{}', "
        "body length: {} bytes, body: '{}'",
        masked_uri, static_cast<int>(http_status), scrubSecrets(reason, secrets), body.length(), scrubSecrets(body, secrets));
}

}
