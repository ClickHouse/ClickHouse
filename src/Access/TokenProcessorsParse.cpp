#include "TokenProcessors.h"

#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <Poco/String.h>
#include <Poco/URI.h>

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
    auto expected_issuer = config.getString(prefix + ".expected_issuer", "");
    auto expected_audience = config.getString(prefix + ".expected_audience", "");
    /// `expected_typ` is the JWT header `typ` to require. RFC 8725 §3.11 and
    /// RFC 9068 recommend type discrimination to prevent cross-token-class
    /// substitution -- e.g. accepting an ID token (intended for client login)
    /// where an access token (intended for resource access) is expected.
    /// Common values: "at+jwt" (RFC 9068 access tokens), "JWT" (generic).
    /// Empty (the default) means no `typ` enforcement; the JWT processors warn
    /// at startup when this is left empty so the gap is visible.
    auto expected_typ = config.getString(prefix + ".expected_typ", "");
    auto allow_no_expiration = config.getBool(prefix + ".allow_no_expiration", false);

    /// Constrain every OIDC/JWT trust-chain fetch (discovery, userinfo,
    /// introspection, JWKS) to the operator-approved <remote_url_allow_hosts>.
    ///
    /// Without this gate, any URL the operator pastes into the processor config
    /// -- and any URL returned by an OIDC discovery document -- is fetched
    /// blindly. A misconfigured or attacker-influenced discovery response can
    /// then redirect token validation through hosts the operator never approved.
    ///
    /// We pre-validate every URL the operator typed into the processor config
    /// here, at parse time, so a bad config fails fast at startup rather than
    /// at first authentication. Discovery-derived URLs (jwks_uri etc.) are
    /// validated separately, after the discovery fetch, inside the processor.
    ///
    /// If <remote_url_allow_hosts> is absent the filter degrades to its
    /// historical permissive behavior: this matches every other ClickHouse
    /// outbound URL site and avoids breaking existing deployments.
    RemoteHostFilter remote_host_filter;
    remote_host_filter.setValuesFromConfig(config);

    auto require_allowed_url = [&](const String & raw_url, const char * param_name)
    {
        if (raw_url.empty())
            return;
        try
        {
            remote_host_filter.checkURL(Poco::URI(raw_url));
        }
        catch (const Exception & e)
        {
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                "Token processor '{}': '{}' URL '{}' is not in <remote_url_allow_hosts>: {}",
                                processor_name, param_name, raw_url, e.message());
        }
    };

    if (provider_type == "google")
    {
        return std::make_unique<GoogleTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim, expected_audience);
    }
    else if (provider_type == "azure")
    {
        return std::make_unique<AzureTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim, expected_audience);
    }
    else if (provider_type == "openid")
    {
        auto verifier_leeway = config.getUInt64(prefix + ".verifier_leeway", 60);
        auto jwks_cache_lifetime = config.getUInt64(prefix + ".jwks_cache_lifetime", 3600);

        /// `token_introspection_endpoint` is currently unused at runtime: the
        /// processor relies on JWT-local validation (when JWKS is configured)
        /// or on userinfo, never on RFC 7662 introspection. Don't require it
        /// for "locally configured" mode -- forcing operators to set a value
        /// that does nothing is a footgun. If introspection is wired up later,
        /// the field is already plumbed and can become required at that point.
        bool externally_configured = config.hasProperty(prefix + ".configuration_endpoint") && !config.hasProperty(prefix + ".jwks_uri");
        bool locally_configured = config.hasProperty(prefix + ".userinfo_endpoint");

        if (externally_configured && ! locally_configured)
        {
            const auto configuration_endpoint = config.getString(prefix + ".configuration_endpoint");
            require_allowed_url(configuration_endpoint, "configuration_endpoint");
            /// Opt-out for the HTTPS-on-discovery-returned-URLs check. False by
            /// default; operators who knowingly run an IdP over plain HTTP can
            /// enable it without falling back to manual trust-chain config.
            const auto allow_http_discovery_urls = config.getBool(prefix + ".allow_http_discovery_urls", false);
            return std::make_unique<OpenIdTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                          expected_issuer, expected_audience, allow_no_expiration,
                                                          configuration_endpoint,
                                                          verifier_leeway,
                                                          jwks_cache_lifetime,
                                                          remote_host_filter,
                                                          allow_http_discovery_urls);
        }
        else if (locally_configured && !externally_configured)
        {
            const auto userinfo_endpoint = config.getString(prefix + ".userinfo_endpoint");
            const auto token_introspection_endpoint = config.getString(prefix + ".token_introspection_endpoint", "");
            const auto jwks_uri = config.getString(prefix + ".jwks_uri", "");
            require_allowed_url(userinfo_endpoint, "userinfo_endpoint");
            require_allowed_url(token_introspection_endpoint, "token_introspection_endpoint");
            require_allowed_url(jwks_uri, "jwks_uri");
            return std::make_unique<OpenIdTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                          expected_issuer, expected_audience, allow_no_expiration,
                                                          userinfo_endpoint,
                                                          token_introspection_endpoint,
                                                          verifier_leeway,
                                                          jwks_uri,
                                                          jwks_cache_lifetime);
        }

        throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "Either 'configuration_endpoint' or 'userinfo_endpoint' "
                            "(and, optionally, 'token_introspection_endpoint' / 'jwks_uri') must be specified for 'openid' processor");
    }
    else if (provider_type == "jwt_static_key")
    {
        if (!config.hasProperty(prefix + ".static_key"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'static_key' must be specified for 'jwt_static_key' processor");

        if (!config.hasProperty(prefix + ".algo"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'algo' must be specified for 'jwt_static_key' processor");

        StaticKeyJwtParams params = {Poco::toLower(config.getString(prefix + ".algo")),
                                     config.getString(prefix + ".static_key", ""),
                                     config.getBool(prefix + ".static_key_in_base64", false),
                                     config.getString(prefix + ".public_key", ""),
                                     config.getString(prefix + ".private_key", ""),
                                     config.getString(prefix + ".public_key_password", ""),
                                     config.getString(prefix + ".private_key_password", ""),
                                     config.getString(prefix + ".claims", ""),
                                     config.getUInt64(prefix + ".verifier_leeway", 60)};
        return std::make_unique<StaticKeyJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim, expected_issuer, expected_audience, expected_typ, allow_no_expiration, params);
    }
    else if (provider_type == "jwt_static_jwks")
    {
        if (config.hasProperty(prefix + ".static_jwks") && config.hasProperty(prefix + ".static_jwks_file"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'static_jwks' and 'static_jwks_file' cannot be specified simultaneously for 'jwt_static_jwks' processor");

        if (!config.hasProperty(prefix + ".static_jwks") && !config.hasProperty(prefix + ".static_jwks_file"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'static_jwks' or 'static_jwks_file' must be specified for 'jwt_static_jwks' processor");

        if (config.hasProperty(prefix + ".jwks_uri"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'jwks_uri' cannot be specified for 'jwt_static_jwks' processor");

        StaticJWKSParams params
        {
            config.getString(prefix + ".static_jwks", ""),
            config.getString(prefix + ".static_jwks_file", "")
        };
        return std::make_unique<JwksJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                  expected_issuer, expected_audience, expected_typ, allow_no_expiration,
                                                  config.getString(prefix + ".claims", ""),
                                                  config.getUInt64(prefix + ".verifier_leeway", 60),
                                                  std::make_shared<StaticJWKS>(params));
    }
    if (provider_type == "jwt_dynamic_jwks")
    {
        if (config.hasProperty(prefix + ".static_jwks") || config.hasProperty(prefix + ".static_jwks_file"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'static_jwks' and 'static_jwks_file' cannot be specified for 'jwt_dynamic_jwks' processor");
        if (!config.hasProperty(prefix + ".jwks_uri"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'jwks_uri' must be specified for 'jwt_dynamic_jwks' processor");

        const auto jwks_uri = config.getString(prefix + ".jwks_uri");
        require_allowed_url(jwks_uri, "jwks_uri");
        return std::make_unique<JwksJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                  expected_issuer, expected_audience, expected_typ, allow_no_expiration,
                                                  config.getString(prefix + ".claims", ""),
                                                  config.getUInt64(prefix + ".verifier_leeway", 60),
                                                  jwks_uri,
                                                  config.getUInt(prefix + ".jwks_cache_lifetime", 3600));
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
