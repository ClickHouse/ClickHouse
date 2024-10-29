#include <config.h>

#if USE_JWT_CPP
#include <base/types.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>
#include <shared_mutex>

#include <Poco/URI.h>

namespace DB
{

/// JWKS (JSON Web Key Set) is a kind of a set of public keys that are used to validate JWT authenticity locally.
/// They are usually exposed by identity providers (e.g. Keycloak) via a well-known URI (usually /.well-known/jwks.json)
/// This interface is responsible for managing JWKS. Retrieving, caching and refreshing of JWKS happens here.
/// JWKS can either be static (e.g. provided in config) or dynamic (fetched from a remote URI and).
class IJWKSProvider
{
public:
    virtual ~IJWKSProvider() = default;

    virtual jwt::jwks<jwt::traits::kazuho_picojson> getJWKS() = 0;
};

class JWKSClient : public IJWKSProvider
{
public:
    explicit JWKSClient(const String & uri, const size_t refresh_ms_): refresh_timeout(refresh_ms_), jwks_uri(uri) {}

    ~JWKSClient() override = default;
    JWKSClient(const JWKSClient &) = delete;
    JWKSClient(JWKSClient &&) = delete;
    JWKSClient &operator=(const JWKSClient &) = delete;
    JWKSClient &operator=(JWKSClient &&) = delete;

    jwt::jwks<jwt::traits::kazuho_picojson> getJWKS() override;

private:
    size_t refresh_timeout;
    Poco::URI jwks_uri;

    std::shared_mutex mutex;
    jwt::jwks<jwt::traits::kazuho_picojson> cached_jwks;
    std::chrono::time_point<std::chrono::high_resolution_clock> last_request_send;
};

struct StaticJWKSParams
{
    StaticJWKSParams(const std::string &static_jwks_, const std::string &static_jwks_file_);

    String static_jwks;
    String static_jwks_file;
};

class StaticJWKS : public IJWKSProvider
{
public:
    explicit StaticJWKS(const StaticJWKSParams &params);

private:
    jwt::jwks<jwt::traits::kazuho_picojson> getJWKS() override
    {
        return jwks;
    }

    jwt::jwks<jwt::traits::kazuho_picojson> jwks;
};

}
#endif
