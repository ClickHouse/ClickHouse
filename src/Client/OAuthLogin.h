#pragma once

#include <config.h>
#include <memory>
#include <string>

namespace DB
{

class JWTProvider; // forward declaration — full type available with USE_JWT_CPP && USE_SSL

enum class OAuthFlowMode
{
    AuthCode,
    Device,
};

struct OAuthCredentials
{
    std::string client_id;
    std::string client_secret;
    std::string auth_uri;        // authorization_endpoint
    std::string token_uri;       // token_endpoint
    std::string device_auth_uri; // device_authorization_endpoint (discovered if empty)
    std::string issuer;          // OIDC issuer URL (optional; used to locate discovery document)
};

/// Load from Google-format JSON credentials file.
/// Throws if file not found or malformed.
OAuthCredentials loadOAuthCredentials(const std::string & path);

/// Run OAuth flow, return ID token. Throws on failure.
std::string obtainIDToken(const OAuthCredentials & creds, OAuthFlowMode mode);

#if USE_JWT_CPP && USE_SSL
/// Create a JWTProvider that runs the initial OAuth flow and then silently
/// refreshes the id_token via the cached refresh token for the lifetime
/// of the session.  Assign the result to Client::jwt_provider so that
/// Connection::sendQuery can call getJWT() on each query.
std::shared_ptr<JWTProvider> createOAuthJWTProvider(
    const OAuthCredentials & creds, OAuthFlowMode mode);
#endif

}
