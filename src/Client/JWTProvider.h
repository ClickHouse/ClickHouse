#pragma once

#if USE_JWT_CPP && USE_SSL
#include <config.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Timestamp.h>
#include <Poco/URI.h>

#include <iosfwd>
#include <map>
#include <memory>
#include <string>

namespace DB
{

class JWTProvider
{
public:
    JWTProvider(
        std::string auth_url,
        std::string client_id,
        std::ostream & out,
        std::ostream & err);
    virtual ~JWTProvider() = default;

    /// Returns a valid ClickHouse JWT.
    /// Implementations are responsible for handling the entire lifecycle,
    /// including initial login and subsequent refreshes.
    virtual std::string getJWT();
    static Poco::Timestamp getJwtExpiry(const std::string & token);

protected:
    void deviceCodeLogin();
    void refreshIdPAccessToken();

    static std::unique_ptr<Poco::Net::HTTPSClientSession> createHTTPSession(const Poco::URI & uri);
    static void openURLInBrowser(const std::string & url);

    // Configuration
    std::string auth_url_str;
    std::string client_id_str;
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
    const std::string & host,
    std::ostream & out,
    std::ostream & err);

bool isCloudEndpoint(const std::string & host);
}

#endif
