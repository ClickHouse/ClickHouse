#pragma once

#include <optional>
#include <string>
#include <vector>


namespace DB
{

/// Endpoints used by the OAuth 2.0 Device Authorization Grant (RFC 8628).
struct OAuthDeviceFlowEndpoints
{
    std::string device_authorization_endpoint;
    std::string token_endpoint;
};

/// Remove trailing slashes from an OAuth issuer / authorization-server base URL.
std::string normalizeOAuthIssuerURL(std::string issuer);

/// Candidate discovery document URLs for `issuer` (OIDC append style + RFC 8414 insertion style).
std::vector<std::string> buildOAuthDiscoveryURLs(const std::string & issuer);

/// Parse `device_authorization_endpoint` and `token_endpoint` from a discovery JSON document.
std::optional<OAuthDeviceFlowEndpoints> parseOAuthDiscoveryDocument(const std::string & json_body);

/// Auth0-compatible endpoint layout used when discovery is unavailable.
OAuthDeviceFlowEndpoints auth0StyleOAuthEndpoints(const std::string & issuer);

/// Apply optional explicit endpoint overrides on top of discovered / fallback endpoints.
OAuthDeviceFlowEndpoints applyOAuthEndpointOverrides(
    OAuthDeviceFlowEndpoints endpoints,
    const std::string & device_authorization_endpoint_override,
    const std::string & token_endpoint_override);

/// Resolve the browser verification URL from a device-authorization response.
/// Prefers `verification_uri_complete`, otherwise builds from `verification_uri` + `user_code`.
std::string resolveDeviceVerificationURI(
    const std::string & verification_uri_complete,
    const std::string & verification_uri,
    const std::string & user_code);

/// Default device-code scope used when `--oauth-scope` is not set (Auth0 / Cloud compatible).
inline constexpr const char * default_oauth_device_code_scope = "openid profile email offline_access";

}
