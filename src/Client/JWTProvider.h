#pragma once

#if USE_JWT_CPP && USE_SSL
#include <config.h>

#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Timestamp.h>
#include <Poco/URI.h>


namespace DB
{

class JWTProvider
{
public:
    JWTProvider(
        std::string auth_url,
        std::string client_id,
        std::string audience,
        std::ostream & out,
        std::ostream & err);
    virtual ~JWTProvider() = default;

    /// Returns a valid ClickHouse JWT.
    /// Implementations are responsible for handling the entire lifecycle,
    /// including initial login and subsequent refreshes.
    virtual std::string getJWT();
    static Poco::Timestamp getJwtExpiry(const std::string & token);

protected:
    virtual std::string getAudience() const { return oauth_audience; }
    void deviceCodeLogin();
    void refreshIdPAccessToken();

    static std::unique_ptr<Poco::Net::HTTPSClientSession> createHTTPSession(const Poco::URI & uri);
    static void openURLInBrowser(const std::string & url);

    // Configuration
    std::string oauth_url;
    std::string oauth_client_id;
    std::string oauth_audience;
    std::ostream & output_stream;
    std::ostream & error_stream;

    // Token State
    std::string idp_access_token;
    std::string idp_refresh_token;
    Poco::Timestamp idp_access_token_expires_at{0};

};

/// Creates the appropriate JWT provider based on the application configuration.
std::unique_ptr<JWTProvider> createJwtProvider(
    const std::string & auth_url,
    const std::string & client_id,
    const std::string & audience,
    const std::string & host,
    std::ostream & out,
    std::ostream & err);

}

#endif
