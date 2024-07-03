#pragma once

#include <base/types.h>

#include <chrono>
#include <shared_mutex>

#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>

#include "Access/HTTPAuthClient.h"

namespace DB
{

struct JWKSAuthClientParams: public HTTPAuthClientParams
{
    size_t refresh_ms;
};

class JWKSResponseParser
{
    static constexpr auto settings_key = "settings";
public:
    struct Result
    {
        bool is_ok = false;
        jwt::jwks<jwt::traits::kazuho_picojson> keys;
    };

    Result parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const;
};

class JWKSClient: private HTTPAuthClient<JWKSResponseParser>
{
public:
    explicit JWKSClient(const JWKSAuthClientParams & params_);
    ~JWKSClient();

    JWKSClient(const JWKSClient &) = delete;
    JWKSClient(JWKSClient &&) = delete;
    JWKSClient & operator= (const JWKSClient &) = delete;
    JWKSClient & operator= (JWKSClient &&) = delete;

    bool verify(const String &claims, const String &token);
private:
    jwt::jwks<jwt::traits::kazuho_picojson> getJWKS();

    size_t m_refresh_ms;

    std::shared_mutex m_update_mutex;
    jwt::jwks<jwt::traits::kazuho_picojson> m_jwks;
    std::chrono::time_point<std::chrono::high_resolution_clock> m_last_request_send;
};

}
