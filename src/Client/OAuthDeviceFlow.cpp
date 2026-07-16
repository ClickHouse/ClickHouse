#include <Client/OAuthDeviceFlow.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/StringUtils.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>

#include <algorithm>
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

std::optional<OAuthDeviceFlowEndpoints> parseOAuthDiscoveryDocument(const std::string & json_body)
{
    try
    {
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

std::string resolveDeviceVerificationURI(
    const std::string & verification_uri_complete,
    const std::string & verification_uri,
    const std::string & user_code)
{
    if (!verification_uri_complete.empty())
        return verification_uri_complete;

    if (verification_uri.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Device authorization response is missing both verification_uri_complete and verification_uri");
    }

    if (user_code.empty())
        return verification_uri;

    Poco::URI uri(verification_uri);
    uri.addQueryParameter("user_code", user_code);
    return uri.toString();
}

}
