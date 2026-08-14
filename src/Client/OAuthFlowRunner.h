#pragma once

#include <config.h>
#include <Client/OAuthLogin.h>

#if USE_JWT_CPP && USE_SSL

#include <Poco/JSON/Object.h>

#include <cstddef>
#include <iosfwd>
#include <string>

namespace DB
{

/// Hard cap on the size of an HTTP response body buffered into memory from an
/// OAuth2 / OIDC endpoint. Real responses are a few hundred bytes to a few KiB;
/// the 1 MiB cap stops a hostile or misconfigured endpoint from streaming an
/// unbounded body into a `std::string` (memory-exhaustion DoS of
/// `clickhouse-client`). Anything larger is treated as a protocol error.
/// Applies to the browser (authorization-code) and device login flows
/// implemented in `OAuthFlowRunner.cpp` and `OAuthProviderPolicy.cpp` via
/// `copyStreamWithLimit`.
constexpr std::size_t OAUTH_MAX_RESPONSE_BYTES = 1 * 1024 * 1024;

/// Timeout (seconds) for every HTTP request to an OAuth2 / OIDC endpoint, so a
/// hung or slow endpoint cannot stall `clickhouse-client` indefinitely. Shared
/// by the browser, device, and discovery flows.
constexpr int OAUTH_HTTP_TIMEOUT_SECONDS = 30;

/// Read up to max_bytes from `in` into `out`, throwing `AUTHENTICATION_FAILED`
/// if the stream carries more than max_bytes. Used to bound response sizes from
/// untrusted OAuth/OIDC endpoints.
void copyStreamWithLimit(std::istream & in, std::string & out, std::size_t max_bytes);

std::string urlEncodeOAuth(const std::string & value);
Poco::JSON::Object::Ptr postOAuthForm(const std::string & url, const std::string & body);

/// Build the form body of the RFC 8628 device authorization request. Exposed
/// (rather than kept local to `runOAuthDeviceFlow`) only so the regression
/// tests can verify that a confidential client's `client_secret` is included
/// (RFC 8628 §3.1) while public clients omit the parameter entirely.
std::string buildDeviceAuthorizationRequestBody(const OAuthCredentials & creds, const std::string & scope);

std::string runOAuthAuthCodeFlow(const OAuthCredentials & creds);
std::string runOAuthDeviceFlow(OAuthCredentials creds);

/// Defined in OAuthLogin.cpp, used by the flow runners to persist the refresh token.
void writeCachedRefreshToken(const std::string & client_id, const std::string & refresh_token);

}

#endif // USE_JWT_CPP && USE_SSL
