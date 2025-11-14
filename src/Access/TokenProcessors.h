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
                             const String & username_claim_ = "sub")
    : processor_name(processor_name_), token_cache_lifetime(token_cache_lifetime_), username_claim(username_claim_) {}
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
};

#if USE_JWT_CPP

struct StaticKeyJwtParams
{
    /// Algorithm name (required). Supported: "none", "hs256", "hs384", "hs512", 
    /// "ps256", "ps384", "ps512", "ed25519", "ed448", "rs256", "rs384", "rs512",
    /// "es256", "es256k", "es384", "es512"
    String algo;

    /// For HS algorithms (hs256, hs384, hs512): symmetric key (required for HS algorithms)
    String static_key;

    /// For HS algorithms: whether static_key is base64 encoded (optional, defaults to false)
    bool static_key_in_base64 = false;

    /// For PS/ED/RSA/ES algorithms: public key (required for PS/ED/RSA/ES algorithms)
    String public_key;

    /// For PS/ED/RSA/ES algorithms: private key (optional)
    String private_key;

    /// For PS/ED/RSA/ES algorithms: public key password (optional)
    String public_key_password;

    /// For PS/ED/RSA/ES algorithms: private key password (optional)
    String private_key_password;

    /// JWT claims to validate (optional)
    String claims;
};

class StaticKeyJwtProcessor : public ITokenProcessor
{
public:
    explicit StaticKeyJwtProcessor(const String & processor_name_,
                                   UInt64 token_cache_lifetime_,
                                   const String & username_claim_,
                                   const StaticKeyJwtParams & params);

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
                              const String & claims_,
                              size_t verifier_leeway_,
                              std::shared_ptr<IJWKSProvider> provider_)
                              : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_),
                                claims(claims_), provider(provider_), verifier_leeway(verifier_leeway_) {}

    explicit JwksJwtProcessor(const String & processor_name_,
                              UInt64 token_cache_lifetime_,
                              const String & username_claim_,
                              const String & claims_,
                              size_t verifier_leeway_,
                              const String & jwks_uri_,
                              size_t jwks_cache_lifetime_)
                              : JwksJwtProcessor(processor_name_,
                                                 token_cache_lifetime_,
                                                 username_claim_,
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
#endif

}
