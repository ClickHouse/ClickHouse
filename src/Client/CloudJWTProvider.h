#pragma once

#if USE_JWT_CPP && USE_SSL

#include <Client/JWTProvider.h>
#include <Poco/Timestamp.h>
#include <string>
#include <iosfwd>
#include <map>

namespace DB
{

/// JWT Provider for the ClickHouse managed service flow, which involves a token swap.
class CloudJWTProvider : public JWTProvider
{
public:
    CloudJWTProvider(
        std::string auth_url,
        std::string client_id,
        std::string host,
        std::ostream & out,
        std::ostream & err);

    std::string getJWT() override;

private:
    struct AuthEndpoints
    {
        std::string auth_url;
        std::string client_id;
        std::string api_host;
    };

    void swapIdPTokenForClickHouseJWT(bool show_messages = true);

    static const AuthEndpoints * getAuthEndpoints(const std::string & host);

    // Configuration
    std::string host_str;

    // Token State
    std::string clickhouse_jwt;
    Poco::Timestamp clickhouse_jwt_expires_at{0};

    static const std::map<std::string, AuthEndpoints> managed_service_endpoints;
};

}

#endif
