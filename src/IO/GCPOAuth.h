#pragma once

#include <string>
#include <base/types.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>

namespace DB
{

struct GCPOAuthToken
{
    std::string access_token;
    Int64 expires_in = 3600; /// seconds until expiry as reported by the token endpoint
};

/// Exchange a Google OAuth2 refresh token for an access token by POSTing to
/// https://oauth2.googleapis.com/token.
///
/// All credential values are URL-encoded before being placed in the form body.
/// Session creation is retried up to 5 times.
GCPOAuthToken fetchGCPOAuthToken(
    const std::string & client_id,
    const std::string & client_secret,
    const std::string & refresh_token,
    const ConnectionTimeouts & timeouts,
    HTTPConnectionGroupType group = HTTPConnectionGroupType::HTTP,
    const std::string & token_endpoint = "https://oauth2.googleapis.com/token");

/// Exchange an RS256-signed JWT assertion for an access token
/// (the service account flow, grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer).
/// The token endpoint is a parameter because service account keys carry their own `token_uri`.
GCPOAuthToken fetchGCPOAuthTokenWithJWTAssertion(
    const std::string & assertion,
    const std::string & token_endpoint,
    const ConnectionTimeouts & timeouts,
    HTTPConnectionGroupType group = HTTPConnectionGroupType::HTTP);

/// The OAuth scope that grants access to all Google Cloud APIs the service account is authorized for
/// (both the BigLake Iceberg REST catalog and Cloud Storage).
constexpr auto GCP_CLOUD_PLATFORM_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

struct GCPServiceAccountAssertion
{
    std::string assertion;
    std::string token_endpoint;
};

/// Build an RS256-signed JWT assertion for the OAuth 2.0 service account flow from the content of a
/// Google service account JSON key file (it must have `client_email` and `private_key`).
/// The token endpoint is the key's `token_uri` (Google's default if absent), unless `token_endpoint_override` is set.
/// It comes from user-provided data, so callers must validate it against the allowed hosts before calling
/// `fetchGCPOAuthTokenWithJWTAssertion`.
GCPServiceAccountAssertion makeGCPServiceAccountAssertion(
    const std::string & service_account_key,
    const std::string & scope,
    const std::string & token_endpoint_override = "");

}
