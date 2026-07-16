#include <Client/OAuthDeviceFlow.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/StringUtils.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>

#include <algorithm>
#include <cctype>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void appendUnique(std::vector<std::string> & urls, std::string url)
{
    if (std::find(urls.begin(), urls.end(), url) == urls.end())
        urls.push_back(std::move(url));
}

bool arrayContainsString(const Poco::JSON::Array::Ptr & array, const std::string & value)
{
    if (!array)
        return false;

    for (unsigned int i = 0; i < array->size(); ++i)
    {
        const Poco::Dynamic::Var element = array->get(i);
        if (element.isString() && element.convert<std::string>() == value)
            return true;
    }
    return false;
}

}

std::string normalizeOAuthIssuerURL(std::string issuer)
{
    while (endsWith(issuer, "/") && issuer.size() > strlen("https://"))
        issuer.pop_back();
    return issuer;
}

std::vector<std::string> buildOAuthDiscoveryURLs(const std::string & issuer)
{
    const std::string normalized = normalizeOAuthIssuerURL(issuer);
    if (normalized.empty())
        return {};

    std::vector<std::string> urls;

    /// OpenID Connect Discovery: append `/.well-known/openid-configuration` to the issuer.
    appendUnique(urls, normalized + "/.well-known/openid-configuration");

    /// Append-style OAuth Authorization Server Metadata (common in practice).
    appendUnique(urls, normalized + "/.well-known/oauth-authorization-server");

    /// RFC 8414 insertion style: place `/.well-known/...` between host and issuer path.
    try
    {
        Poco::URI uri(normalized);
        std::string path = uri.getPath();
        if (path == "/")
            path.clear();

        if (!path.empty())
        {
            if (!startsWith(path, "/"))
                path = "/" + path;

            Poco::URI oauth_inserted = uri;
            oauth_inserted.setPath("/.well-known/oauth-authorization-server" + path);
            appendUnique(urls, oauth_inserted.toString());

            Poco::URI oidc_inserted = uri;
            oidc_inserted.setPath("/.well-known/openid-configuration" + path);
            appendUnique(urls, oidc_inserted.toString());
        }
    }
    catch (const Poco::Exception &)
    {
        /// Issuer was not a valid URI; keep the append-style candidates above.
    }

    return urls;
}

bool discoverySupportsDeviceCodeGrant(const std::string & json_body)
{
    try
    {
        Poco::JSON::Object::Ptr object = Poco::JSON::Parser().parse(json_body).extract<Poco::JSON::Object::Ptr>();
        if (!object || !object->has("grant_types_supported"))
            return true; /// Optional field: assume supported when omitted.

        return arrayContainsString(object->getArray("grant_types_supported"), oauth_device_code_grant_type);
    }
    catch (...)
    {
        return true;
    }
}

std::optional<OAuthDeviceFlowEndpoints> parseOAuthDiscoveryDocument(const std::string & json_body)
{
    try
    {
        if (!discoverySupportsDeviceCodeGrant(json_body))
            return std::nullopt;

        Poco::JSON::Object::Ptr object = Poco::JSON::Parser().parse(json_body).extract<Poco::JSON::Object::Ptr>();
        if (!object || !object->has("token_endpoint") || !object->has("device_authorization_endpoint"))
            return std::nullopt;

        OAuthDeviceFlowEndpoints endpoints;
        endpoints.token_endpoint = object->getValue<std::string>("token_endpoint");
        endpoints.device_authorization_endpoint = object->getValue<std::string>("device_authorization_endpoint");

        if (endpoints.token_endpoint.empty() || endpoints.device_authorization_endpoint.empty())
            return std::nullopt;

        return endpoints;
    }
    catch (...)
    {
        return std::nullopt;
    }
}

OAuthDeviceFlowEndpoints auth0StyleOAuthEndpoints(const std::string & issuer)
{
    const std::string normalized = normalizeOAuthIssuerURL(issuer);
    return {
        normalized + "/oauth/device/code",
        normalized + "/oauth/token",
    };
}

OAuthDeviceFlowEndpoints applyOAuthEndpointOverrides(
    OAuthDeviceFlowEndpoints endpoints,
    const std::string & device_authorization_endpoint_override,
    const std::string & token_endpoint_override)
{
    if (!device_authorization_endpoint_override.empty())
        endpoints.device_authorization_endpoint = device_authorization_endpoint_override;
    if (!token_endpoint_override.empty())
        endpoints.token_endpoint = token_endpoint_override;
    return endpoints;
}

std::string encodeFormComponent(const std::string & value)
{
    /// Encode every character that is not RFC 3986 unreserved so that
    /// `application/x-www-form-urlencoded` values match RFC 8628 examples
    /// (e.g. grant_type colons become %3A).
    static const std::string form_reserved = ":/?#[]@!$&'()*+,;=%<>{}|\\\"^`";
    std::string encoded;
    Poco::URI::encode(value, form_reserved, encoded);
    return encoded;
}

std::string buildFormUrlEncodedBody(const std::vector<std::pair<std::string, std::string>> & fields)
{
    std::string body;
    for (const auto & [key, value] : fields)
    {
        if (value.empty())
            continue;
        if (!body.empty())
            body += '&';
        body += encodeFormComponent(key);
        body += '=';
        body += encodeFormComponent(value);
    }
    return body;
}

std::optional<OAuthError> parseOAuthErrorResponse(const std::string & json_body)
{
    try
    {
        Poco::JSON::Object::Ptr object = Poco::JSON::Parser().parse(json_body).extract<Poco::JSON::Object::Ptr>();
        if (!object || !object->has("error"))
            return std::nullopt;

        OAuthError error;
        error.error = object->getValue<std::string>("error");
        error.error_description = object->optValue<std::string>("error_description", "");
        return error;
    }
    catch (...)
    {
        return std::nullopt;
    }
}

std::string formatOAuthError(const OAuthError & error)
{
    if (error.error_description.empty())
        return error.error;
    return error.error + ": " + error.error_description;
}

std::string formatOAuthError(const std::string & response_body, int status, const std::string & reason)
{
    if (auto parsed = parseOAuthErrorResponse(response_body))
        return formatOAuthError(*parsed);

    if (!response_body.empty())
        return std::to_string(status) + " " + reason + ": " + response_body;

    return std::to_string(status) + " " + reason;
}

std::string formatDeviceLoginInstructions(
    const std::string & verification_uri,
    const std::string & user_code,
    const std::string & verification_uri_complete)
{
    if (verification_uri.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Device authorization response is missing required verification_uri");
    }
    if (user_code.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Device authorization response is missing required user_code");
    }

    std::string message;
    message += "\nUsing a browser on another device, visit:\n\n";
    message += "        ";
    message += verification_uri;
    message += "\n\nAnd enter the code: \033[1m";
    message += user_code;
    message += "\033[0m\n";

    if (!verification_uri_complete.empty())
    {
        message += "\nShortcut URL (optional, includes the code):\n\n";
        message += "        ";
        message += verification_uri_complete;
        message += "\n";
    }

    message += "\n";
    return message;
}

std::string browserVerificationURL(
    const std::string & verification_uri_complete,
    const std::string & verification_uri)
{
    if (!verification_uri_complete.empty())
        return verification_uri_complete;
    return verification_uri;
}

int nextPollingIntervalAfterConnectionFailure(int current_interval_seconds)
{
    /// RFC 8628 Section 3.5: exponential backoff, recommended cap around one minute.
    constexpr int max_interval_seconds = 60;
    if (current_interval_seconds <= 0)
        return 5;
    return std::min(current_interval_seconds * 2, max_interval_seconds);
}

}
