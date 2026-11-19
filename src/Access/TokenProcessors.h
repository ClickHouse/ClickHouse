#pragma once

#include <Access/Credentials.h>
#include <Poco/Util/AbstractConfiguration.h>

#if USE_JWT_CPP
#include <Access/Common/JWKSProvider.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

class ITokenProcessor
{
public:
    explicit ITokenProcessor(const String & processor_name_,
                             UInt64 token_cache_lifetime_,
                             const String & username_claim_ = "sub",
                             const String & groups_claim_ = "groups")
    : processor_name(processor_name_), token_cache_lifetime(token_cache_lifetime_), username_claim(username_claim_), groups_claim(groups_claim_) {}
    virtual ~ITokenProcessor() = default;

    virtual bool resolveAndValidate(TokenCredentials &) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for ITokenProcessor interface");
    }

    virtual bool checkClaims(const TokenCredentials &, const String &) { return true; }

    UInt64 getTokenCacheLifetime() const { return token_cache_lifetime; }
    String getProcessorName() const { return processor_name; }

    static std::unique_ptr<DB::ITokenProcessor> parseTokenProcessor(
            const Poco::Util::AbstractConfiguration & config,
            const String & prefix,
            const String & processor_name);

protected:
    const String processor_name;
    const UInt64 token_cache_lifetime;
    const String username_claim;
    const String groups_claim;
};

#if USE_JWT_CPP
class StaticKeyJwtProcessor : public ITokenProcessor
{
public:
    explicit StaticKeyJwtProcessor(const String & processor_name_,
                                   UInt64 token_cache_lifetime_,
                                   const String & username_claim_,
                                   const String & groups_claim_,
                                   const String & claims_,
                                   const String & algo,
                                   const String & static_key,
                                   bool static_key_in_base64,
                                   const String & public_key,
                                   const String & private_key,
                                   const String & public_key_password,
                                   const String & private_key_password);

    bool resolveAndValidate(TokenCredentials & credentials) const override;
    bool checkClaims(const TokenCredentials & credentials, const String & claims_to_check) override;

private:
    const String claims;
    jwt::verifier<jwt::default_clock, jwt::traits::kazuho_picojson> verifier = jwt::verify();
};


class JwksJwtProcessor : public ITokenProcessor
{
public:
    explicit JwksJwtProcessor(const String & processor_name_,
                              UInt64 token_cache_lifetime_,
                              const String & username_claim_,
                              const String & groups_claim_,
                              const String & claims_,
                              size_t verifier_leeway_,
                              std::shared_ptr<IJWKSProvider> provider_)
                              : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_),
                                claims(claims_), provider(provider_), verifier_leeway(verifier_leeway_) {}

    explicit JwksJwtProcessor(const String & processor_name_,
                              UInt64 token_cache_lifetime_,
                              const String & username_claim_,
                              const String & groups_claim_,
                              const String & claims_,
                              size_t verifier_leeway_,
                              const String & jwks_uri_,
                              size_t jwks_cache_lifetime_)
                              : JwksJwtProcessor(processor_name_,
                                                 token_cache_lifetime_,
                                                 username_claim_,
                                                 groups_claim_,
                                                 claims_,
                                                 verifier_leeway_,
                                                 std::make_shared<JWKSClient>(jwks_uri_, jwks_cache_lifetime_)) {}

    bool resolveAndValidate(TokenCredentials & credentials) const override;
    bool checkClaims(const TokenCredentials & credentials, const String & claims_to_check) override;

private:
    const String claims;
    mutable jwt::verifier<jwt::default_clock, jwt::traits::kazuho_picojson> verifier = jwt::verify();
    std::shared_ptr<IJWKSProvider> provider;
    const size_t verifier_leeway;
};

/// Opaque tokens

class GoogleTokenProcessor : public ITokenProcessor
{
public:
    GoogleTokenProcessor(const String & processor_name_,
                         UInt64 token_cache_lifetime_,
                         const String & username_claim_,
                         const String & groups_claim_)
            : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_) {}

    bool resolveAndValidate(TokenCredentials & credentials) const override;
};

class AzureTokenProcessor : public ITokenProcessor
{
public:
    AzureTokenProcessor(const String & processor_name_,
                        UInt64 token_cache_lifetime_,
                        const String & username_claim_,
                        const String & groups_claim_)
            : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_) {}

    bool resolveAndValidate(TokenCredentials & credentials) const override;
};

class OpenIdTokenProcessor : public ITokenProcessor
{
public:
    /// Specify endpoints manually
    OpenIdTokenProcessor(const String & processor_name_,
                         UInt64 token_cache_lifetime_,
                         const String & username_claim_,
                         const String & groups_claim_,
                         const String & userinfo_endpoint_,
                         const String & token_introspection_endpoint_,
                         UInt64 verifier_leeway_,
                         const String & jwks_uri_,
                         UInt64 jwks_cache_lifetime_);

    /// Obtain endpoints from openid-configuration URL
    OpenIdTokenProcessor(const String & processor_name_,
                         UInt64 token_cache_lifetime_,
                         const String & username_claim_,
                         const String & groups_claim_,
                         const String & openid_config_endpoint_,
                         UInt64 verifier_leeway_,
                         UInt64 jwks_cache_lifetime_);

    bool resolveAndValidate(TokenCredentials & credentials) const override;
private:
    Poco::URI userinfo_endpoint;
    Poco::URI token_introspection_endpoint;

    /// Access token is often a valid JWT, so we can validate it locally to avoid unnecesary network requests.
    std::optional<JwksJwtProcessor> jwt_validator = std::nullopt;
};

#endif

}
