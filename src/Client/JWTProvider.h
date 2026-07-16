#pragma once

#if USE_JWT_CPP && USE_SSL
#include <config.h>

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Timestamp.h>
#include <Poco/URI.h>

#include <memory>
#include <ostream>
#include <string>


namespace DB
{

/// Configuration for interactive OAuth device-code login (`clickhouse-client --login`).
struct JWTProviderOptions
{
    std::string auth_url; /// OAuth / OIDC issuer base URL (`--oauth-url`)
    std::string client_id;
    std::string audience;
    std::string scope; /// Empty => default Auth0 / Cloud compatible scope
    std::string device_authorization_endpoint; /// Optional explicit override (`--oauth-device-uri`)
    std::string token_endpoint; /// Optional explicit override (`--oauth-token-uri`)
};

class JWTProvider
{
public:
    JWTProvider(
        JWTProviderOptions options,
        std::ostream & out,
        std::ostream & err);
    virtual ~JWTProvider() = default;

    /// Returns a valid JWT for authenticating to ClickHouse.
    /// Implementations are responsible for handling the entire lifecycle,
    /// including initial login and subsequent refreshes.
    virtual std::string getJWT();
    static Poco::Timestamp getJwtExpiry(const std::string & token);

protected:
    virtual std::string getAudience() const { return oauth_audience; }
    void deviceCodeLogin();
    void refreshIdPAccessToken();

    /// Resolve device + token endpoints via overrides, OIDC/OAuth discovery, or Auth0-style fallback.
    void ensureOAuthEndpointsResolved();

    static std::unique_ptr<Poco::Net::HTTPSClientSession> createHTTPSession(const Poco::URI & uri);
    static void openURLInBrowser(const std::string & url);
    static std::string httpGet(const Poco::URI & uri);
    void storeAccessTokenFromResponse(const Poco::JSON::Object::Ptr & token_object);

    // Configuration
    std::string oauth_url;
    std::string oauth_client_id;
    std::string oauth_audience;
    std::string oauth_scope;
    std::string oauth_device_authorization_endpoint_override;
    std::string oauth_token_endpoint_override;
    std::string resolved_device_authorization_endpoint;
    std::string resolved_token_endpoint;
    bool oauth_endpoints_resolved = false;
    std::ostream & output_stream;
    std::ostream & error_stream;

    // Token State
    std::string idp_access_token;
    std::string idp_refresh_token;
    Poco::Timestamp idp_access_token_expires_at{0};
};

/// Creates the appropriate JWT provider based on the application configuration.
std::unique_ptr<JWTProvider> createJwtProvider(
    JWTProviderOptions options,
    const std::string & host,
    std::ostream & out,
    std::ostream & err);

}

#endif
