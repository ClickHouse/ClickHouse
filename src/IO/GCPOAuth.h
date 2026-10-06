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
    HTTPConnectionGroupType group = HTTPConnectionGroupType::HTTP);

}
