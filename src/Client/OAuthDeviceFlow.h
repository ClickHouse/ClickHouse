#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace DB
{

/// Endpoints used by the OAuth 2.0 Device Authorization Grant (RFC 8628).
struct OAuthDeviceFlowEndpoints
{
    std::string device_authorization_endpoint;
    std::string token_endpoint;
};

/// Parsed OAuth error object (RFC 6749 Section 5.2).
struct OAuthError
{
    std::string error;
    std::string error_description;
};

/// How a confidential client authenticates to the device/token endpoints.
enum class OAuthClientAuthMethod
{
    None, /// Public client: only `client_id` in the body
    Basic, /// `client_secret_basic` (HTTP Basic)
    Post, /// `client_secret_post` (`client_secret` in the body)
};

/// Remove trailing slashes from an OAuth issuer / authorization-server base URL.
std::string normalizeOAuthIssuerURL(std::string issuer);

/// Candidate discovery document URLs for `issuer` (OIDC append style + RFC 8414 insertion style).
std::vector<std::string> buildOAuthDiscoveryURLs(const std::string & issuer);

/// Parse `device_authorization_endpoint` and `token_endpoint` from a discovery JSON document.
/// If `grant_types_supported` is present, requires the RFC 8628 device_code grant type.
std::optional<OAuthDeviceFlowEndpoints> parseOAuthDiscoveryDocument(const std::string & json_body);

/// Returns true when discovery metadata lists (or omits) support for the device_code grant.
bool discoverySupportsDeviceCodeGrant(const std::string & json_body);

/// Auth0-compatible endpoint layout used when discovery is unavailable.
OAuthDeviceFlowEndpoints auth0StyleOAuthEndpoints(const std::string & issuer);

/// Apply optional explicit endpoint overrides on top of discovered / fallback endpoints.
OAuthDeviceFlowEndpoints applyOAuthEndpointOverrides(
    OAuthDeviceFlowEndpoints endpoints,
    const std::string & device_authorization_endpoint_override,
    const std::string & token_endpoint_override);

/// Percent-encode a single `application/x-www-form-urlencoded` component.
std::string encodeFormComponent(const std::string & value);

/// Build an `application/x-www-form-urlencoded` body. Empty values are omitted.
std::string buildFormUrlEncodedBody(const std::vector<std::pair<std::string, std::string>> & fields);

/// Parse RFC 6749 Section 5.2 error JSON; returns nullopt if not parseable.
std::optional<OAuthError> parseOAuthErrorResponse(const std::string & json_body);

/// Format a human-readable OAuth error for exceptions / logs.
std::string formatOAuthError(const OAuthError & error);
std::string formatOAuthError(const std::string & response_body, int status, const std::string & reason);

/// RFC 8628 Section 3.3 user instructions: always show short verification_uri + user_code.
/// When `verification_uri_complete` is set, also mention the shortcut URL.
std::string formatDeviceLoginInstructions(
    const std::string & verification_uri,
    const std::string & user_code,
    const std::string & verification_uri_complete);

/// URL to open in a browser: prefer complete URI, else short verification_uri.
std::string browserVerificationURL(
    const std::string & verification_uri_complete,
    const std::string & verification_uri);

/// RFC 8628 Section 3.5: on connection timeout/failure, double the interval (cap at 60s).
int nextPollingIntervalAfterConnectionFailure(int current_interval_seconds);

/// RFC 8628 Section 3.5: how the client should react to a non-success token poll response.
enum class DeviceTokenPollAction
{
    ContinuePending,
    ContinueSlowDown,
    FailAccessDenied,
    FailExpiredToken,
    FailOther,
};

struct DeviceTokenPollDecision
{
    DeviceTokenPollAction action = DeviceTokenPollAction::FailOther;
    int interval_seconds = 5;
    std::string message;
};

/// Classify a failed device-code token poll (HTTP non-OK with optional OAuth error JSON).
DeviceTokenPollDecision evaluateDeviceTokenPollFailure(
    const std::string & response_body,
    int http_status,
    const std::string & http_reason,
    int current_interval_seconds);

/// Parse `--oauth-client-auth` for confidential clients. Empty secret => None.
OAuthClientAuthMethod parseOAuthClientAuthMethod(
    const std::string & client_secret,
    const std::string & auth_method);

/// Append `client_secret` for `client_secret_post`.
std::string appendClientSecretPost(std::string body, const std::string & client_secret);

/// Require both endpoint overrides together (or neither).
void validateOAuthEndpointOverridePair(
    const std::string & device_authorization_endpoint_override,
    const std::string & token_endpoint_override);

/// Empty `--oauth-scope` uses the Auth0 / Cloud default.
std::string effectiveOAuthDeviceCodeScope(const std::string & configured_scope);

/// Default device-code scope used when `--oauth-scope` is not set (Auth0 / Cloud compatible).
inline constexpr const char * default_oauth_device_code_scope = "openid profile email offline_access";

inline constexpr const char * oauth_device_code_grant_type = "urn:ietf:params:oauth:grant-type:device_code";

}
