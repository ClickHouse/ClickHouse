#pragma once

#include <base/types.h>

#include <chrono>
#include <memory>
#include <shared_mutex>

#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>

#include "Access/HTTPAuthClient.h"

namespace DB
{

class SettingsChanges;

struct JWTVerifierParams
{
    String settings_key;
};

class IJWTVerifier
{
public:
    explicit IJWTVerifier(const String & _name)
        : name(_name)
    {}
    void init(const JWTVerifierParams &_params);
    bool verify(const String &claims, const String &token, SettingsChanges & settings) const;
    virtual ~IJWTVerifier() = default;
protected:
    virtual bool verify_impl(const jwt::decoded_jwt<jwt::traits::kazuho_picojson> &token) const = 0;
    JWTVerifierParams params;
    const String name;
};

struct SimpleJWTVerifierParams:
    public JWTVerifierParams
{
    String algo;
    String single_key;
    bool single_key_in_base64;
    String public_key;
    String private_key;
    String public_key_password;
    String private_key_password;
    void validate() const;
};

class SimpleJWTVerifier: public IJWTVerifier
{
public:
    explicit SimpleJWTVerifier(const String & _name);
    void init(const SimpleJWTVerifierParams & _params);
private:
    bool verify_impl(const jwt::decoded_jwt<jwt::traits::kazuho_picojson> &token) const override;
    jwt::verifier<jwt::default_clock, jwt::traits::kazuho_picojson> verifier;
};

class IJWKSProvider
{
public:
    virtual ~IJWKSProvider() = default;
    virtual jwt::jwks<jwt::traits::kazuho_picojson> getJWKS() = 0;
};

class JWKSVerifier: public IJWTVerifier
{
public:
    explicit JWKSVerifier(const String & _name, std::shared_ptr<IJWKSProvider> _provider);
private:
    bool verify_impl(const jwt::decoded_jwt<jwt::traits::kazuho_picojson> &token) const override;

    std::shared_ptr<IJWKSProvider> provider;
};

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

class JWKSClient: public IJWKSProvider,
                  private HTTPAuthClient<JWKSResponseParser>
{
public:
    explicit JWKSClient(const JWKSAuthClientParams & params_);
    ~JWKSClient() override;

    JWKSClient(const JWKSClient &) = delete;
    JWKSClient(JWKSClient &&) = delete;
    JWKSClient & operator= (const JWKSClient &) = delete;
    JWKSClient & operator= (JWKSClient &&) = delete;
private:
    jwt::jwks<jwt::traits::kazuho_picojson> getJWKS() override;

    size_t m_refresh_ms;

    std::shared_mutex m_update_mutex;
    jwt::jwks<jwt::traits::kazuho_picojson> m_jwks;
    std::chrono::time_point<std::chrono::high_resolution_clock> m_last_request_send;
};

struct StaticJWKSParams
{
    String static_jwks;
    String static_jwks_file;
    void validate() const;
};

class StaticJWKS: public IJWKSProvider
{
public:
    void init(const StaticJWKSParams& params);
private:
    jwt::jwks<jwt::traits::kazuho_picojson> getJWKS() override
    {
        return jwks;
    }
    jwt::jwks<jwt::traits::kazuho_picojson> jwks;
};

}
