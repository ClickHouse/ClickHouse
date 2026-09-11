#pragma once

#include <string>
#include <base/types.h>
#include <Common/Logger.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>

namespace DB
{

struct GCPOAuthToken
{
    std::string access_token;
    Int64 expires_in = 3600; /// seconds until expiry as reported by the token endpoint
};

/// Open a session to a Google token endpoint. A session taken from the pool may have been closed by the peer
/// meanwhile, which `makeHTTPSession` reports by throwing, so retry a few times before giving up.
HTTPSessionPtr makeGCPTokenEndpointSession(
    HTTPConnectionGroupType group,
    const Poco::URI & url,
    const ConnectionTimeouts & timeouts,
    LoggerPtr log);

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

}
