#include "TokenProcessors.h"

#include <Common/logger_useful.h>
#include <Poco/String.h>

namespace DB {

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
}

#if USE_JWT_CPP
std::unique_ptr<DB::ITokenProcessor> ITokenProcessor::parseTokenProcessor(
        const Poco::Util::AbstractConfiguration & config,
        const String & prefix,
        const String & processor_name)
{
    if (!config.hasProperty(prefix + ".type"))
        throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'type' parameter shall be specified in token_processor configuration.'");

    auto provider_type = Poco::toLower(config.getString(prefix + ".type"));

    auto token_cache_lifetime = config.getUInt64(prefix + ".token_cache_lifetime", 3600);
    auto username_claim = config.getString(prefix + ".username_claim", "sub");
    auto groups_claim = config.getString(prefix + ".groups_claim", "groups");

    if (provider_type == "google")
    {
        return std::make_unique<GoogleTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim);
    }
    else if (provider_type == "azure")
    {
        return std::make_unique<AzureTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim);
    }
    else if (provider_type == "openid")
    {
        auto verifier_leeway = config.getUInt64(prefix + ".verifier_leeway", 60);
        auto jwks_cache_lifetime = config.getUInt64(prefix + ".jwks_cache_lifetime", 3600);

        bool externally_configured = config.hasProperty(prefix + ".configuration_endpoint") && !config.hasProperty(prefix + ".jwks_uri");
        bool locally_configured = config.hasProperty(prefix + ".userinfo_endpoint") && config.hasProperty(prefix + ".token_introspection_endpoint");

        if (externally_configured && ! locally_configured)
        {
            return std::make_unique<OpenIdTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                          config.getString(prefix + ".openid_config_endpoint"),
                                                          verifier_leeway,
                                                          jwks_cache_lifetime);
        }
        else if (locally_configured && !externally_configured)
        {
            return std::make_unique<OpenIdTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                          config.getString(prefix + ".userinfo_endpoint"),
                                                          config.getString(prefix + ".token_introspection_endpoint"),
                                                          verifier_leeway,
                                                          config.getString(prefix + ".jwks_uri", ""),
                                                          jwks_cache_lifetime);
        }

        throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Either 'configuration_endpoint' or both 'userinfo_endpoint' and 'token_introspection_endpoint' (and, optionally, 'jwks_uri') must be specified for 'openid' processor");
    }
    else if (provider_type == "jwt")
    {
        if (config.hasProperty(prefix + ".static_jwks") && config.hasProperty(prefix + ".static_jwks_file"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'static_jwks' and 'static_jwks_file' cannot be specified simultaneously");

        bool is_static_key = config.hasProperty(prefix + ".algo");
        bool is_static_jwks = config.hasProperty(prefix + ".static_jwks") != config.hasProperty(prefix + ".static_jwks_file");
        bool is_remote_jwks = config.hasProperty(prefix + ".jwks_uri");

        if (is_static_key && !is_static_jwks && !is_remote_jwks)  /// StaticKeyJwtProcessor
        {
            return std::make_unique<StaticKeyJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                           config.getString(prefix + ".claims", ""),
                                                           Poco::toLower(config.getString(prefix + ".algo")),
                                                           config.getString(prefix + ".static_key", ""),
                                                           config.getBool(prefix + ".static_key_in_base64", false),
                                                           config.getString(prefix + ".public_key", ""),
                                                           config.getString(prefix + ".private_key", ""),
                                                           config.getString(prefix + ".public_key_password", ""),
                                                           config.getString(prefix + ".private_key_password", ""));
        }
        else if (!is_static_key && is_static_jwks && !is_remote_jwks)
        {
            StaticJWKSParams params
            {
                config.getString(prefix + ".static_jwks", ""),
                config.getString(prefix + ".static_jwks_file", "")
            };
            return std::make_unique<JwksJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                      config.getString(prefix + ".claims", ""),
                                                      config.getUInt64(prefix + ".verifier_leeway", 0),
                                                      std::make_shared<StaticJWKS>(params));
        }
        else if (!is_static_key && !is_static_jwks && is_remote_jwks)
        {
            return std::make_unique<JwksJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                      config.getString(prefix + ".claims", ""),
                                                      config.getUInt64(prefix + ".verifier_leeway", 0),
                                                      config.getString(prefix + ".jwks_uri"),
                                                      config.getUInt(prefix + ".jwks_cache_lifetime", 3600));
        }
        else
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'algo', 'jwks_uri' or 'static_jwks'/'static_jwks_file' must be specified for 'jwt' processor");
    }
    else
        throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid type: {}", provider_type);

    // throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Failed to parse token processor: {}", processor_name);
}

#else
std::unique_ptr<DB::ITokenProcessor> ITokenProcessor::parseTokenProcessor(
    const Poco::Util::AbstractConfiguration &,
    const String &,
    const String &)
{
    throw DB::Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Failed to parse token_processor, ClickHouse was built without JWT support.");
}
#endif

}
