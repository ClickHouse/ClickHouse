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

    /// `azure` is a back-compat alias for `entra`. The legacy `azure` processor
    /// validated tokens by round-tripping through Microsoft Graph; the `entra`
    /// processor does pure local JWKS validation, which is what every operator
    /// actually wants. Treat both names as the same processor type so existing
    /// configs continue to parse, just under stricter validation rules.
    if (provider_type == "azure")
        provider_type = "entra";

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
    else if (provider_type == "openid")
    {
        bool externally_configured = config.hasProperty(prefix + ".configuration_endpoint");
        bool locally_configured = config.hasProperty(prefix + ".userinfo_endpoint");

        if (externally_configured && locally_configured)
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                "Token processor '{}': 'configuration_endpoint' and 'userinfo_endpoint' are mutually exclusive.",
                                processor_name);

        const auto introspection_client_id = config.getString(prefix + ".introspection_client_id", "");
        const auto introspection_client_secret = config.getString(prefix + ".introspection_client_secret", "");
        if (introspection_client_id.empty() != introspection_client_secret.empty())
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                "Token processor '{}': 'introspection_client_id' and 'introspection_client_secret' "
                                "must be configured together.",
                                processor_name);

        auto reject_unsupported_key = [&](const char * key, const char * hint)
        {
            if (config.hasProperty(prefix + "." + key))
                throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                    "Token processor '{}': '{}' is not supported in this mode. {}",
                                    processor_name, key, hint);
        };

        if (externally_configured)
        {
            reject_unsupported_key("jwks_uri",
                "In discovery mode the JWKS URL is resolved from the discovery document; "
                "for an explicit JWKS URL use a 'jwt_dynamic_jwks' processor.");

            auto verifier_leeway = config.getUInt64(prefix + ".verifier_leeway", 60);
            auto jwks_cache_lifetime = config.getUInt64(prefix + ".jwks_cache_lifetime", 3600);
            const auto configuration_endpoint = config.getString(prefix + ".configuration_endpoint");
            require_allowed_url(configuration_endpoint, "configuration_endpoint");
            const auto allow_http_discovery_urls = config.getBool(prefix + ".allow_http_discovery_urls", false);
            return std::make_unique<OpenIdTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                          expected_issuer, expected_audience, allow_no_expiration,
                                                          configuration_endpoint,
                                                          verifier_leeway,
                                                          jwks_cache_lifetime,
                                                          introspection_client_id,
                                                          introspection_client_secret,
                                                          remote_host_filter,
                                                          allow_http_discovery_urls);
        }

        if (locally_configured)
        {
            reject_unsupported_key("jwks_uri",
                "For local JWT validation against a JWKS use a 'jwt_dynamic_jwks' processor.");
            reject_unsupported_key("allow_no_expiration", "It applies only to JWT validation.");
            reject_unsupported_key("verifier_leeway", "It applies only to JWT validation.");
            reject_unsupported_key("jwks_cache_lifetime", "It applies only to JWKS-backed processors.");

            const auto token_introspection_endpoint = config.getString(prefix + ".token_introspection_endpoint", "");
            const bool has_introspection = !token_introspection_endpoint.empty();

            if (has_introspection && introspection_client_id.empty())
                throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                    "Token processor '{}': 'token_introspection_endpoint' is set but "
                                    "'introspection_client_id' / 'introspection_client_secret' are not.",
                                    processor_name);
            if (!has_introspection && !introspection_client_id.empty())
                throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                    "Token processor '{}': 'introspection_client_id' / 'introspection_client_secret' "
                                    "are set but no 'token_introspection_endpoint' is configured.",
                                    processor_name);

            if ((config.hasProperty(prefix + ".expected_issuer") || config.hasProperty(prefix + ".expected_audience"))
                && !has_introspection)
                throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                    "Token processor '{}': 'expected_issuer' / 'expected_audience' need either a "
                                    "'token_introspection_endpoint' (RFC 7662) or a 'jwt_dynamic_jwks' processor.",
                                    processor_name);

            const auto userinfo_endpoint = config.getString(prefix + ".userinfo_endpoint");
            require_allowed_url(userinfo_endpoint, "userinfo_endpoint");
            require_allowed_url(token_introspection_endpoint, "token_introspection_endpoint");
            return std::make_unique<OpenIdTokenProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                          expected_issuer, expected_audience,
                                                          userinfo_endpoint,
                                                          token_introspection_endpoint,
                                                          introspection_client_id,
                                                          introspection_client_secret);
        }

        throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "Either 'configuration_endpoint' (discovery) or 'userinfo_endpoint' (manual) "
                            "must be specified for 'openid' processor");
    }
    else if (provider_type == "entra")
    {
        /// Preset for Microsoft Entra ID built on top of the pure-JWKS JWT processor.
        /// Validation is fully local: signature against Entra's published JWKS plus the
        /// operator-chosen iss/aud/typ/claims pins. No OIDC discovery fetch, no userinfo
        /// endpoint, no Microsoft Graph URL stored on the processor. `groups_claim` and
        /// `username_claim` are read directly from the JWT payload -- which requires the
        /// access token's audience to be the operator's own app, not Microsoft Graph
        /// (Graph-audience tokens are not JWKS-verifiable -- their signing keys are not
        /// in the tenant JWKS and their headers carry a `nonce` that breaks third-party
        /// validation; see `docs/entra-setup-draft.md` for how to mint app-audience tokens).
        if (!config.hasProperty(prefix + ".tenant_id"))
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'tenant_id' must be specified for 'entra' processor");

        const String tenant_id = config.getString(prefix + ".tenant_id");

        if (tenant_id.empty())
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'tenant_id' must not be empty for 'entra' processor");

        for (char c : tenant_id)
        {
            if (!std::isalnum(static_cast<unsigned char>(c)) && c != '-' && c != '.' && c != '_')
                throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                    "'tenant_id' {} contains invalid characters", tenant_id);
        }

        /// Multi-tenant aliases require templated-issuer validation that JwksJwtProcessor does not
        /// implement (it does exact-match on `iss`). Reject explicitly rather than silently failing
        /// issuer checks at token-validation time.
        const String lower_tenant_id = Poco::toLower(tenant_id);
        if (lower_tenant_id == "common" || lower_tenant_id == "organizations" || lower_tenant_id == "consumers")
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                "Multi-tenant 'tenant_id' '{}' is not supported for 'entra' processor type: "
                                "exact issuer validation requires a single tenant identifier (GUID or onmicrosoft.com domain).",
                                tenant_id);

        const String default_jwks_uri = "https://login.microsoftonline.com/" + tenant_id + "/discovery/v2.0/keys";
        const String jwks_uri = config.getString(prefix + ".jwks_uri", default_jwks_uri);
        require_allowed_url(jwks_uri, "jwks_uri");

        /// `expected_issuer` is auto-derived from `tenant_id` since the v2.0 issuer URL is fully
        /// determined by the tenant. Users can still override -- typically for v1.0 tokens
        /// ('https://sts.windows.net/{tenant_id}/') or for sovereign-cloud authorities
        /// ('https://login.microsoftonline.us/{tenant_id}/v2.0' etc.).
        const String default_issuer = "https://login.microsoftonline.com/" + tenant_id + "/v2.0";
        const String issuer = config.getString(prefix + ".expected_issuer", default_issuer);

        if (expected_audience.empty())
            LOG_WARNING(getLogger("TokenAuthentication"),
                        "{}: 'expected_audience' is not set for 'entra' processor: the 'aud' claim will not be validated, "
                        "so tokens issued for any application will be accepted as long as the signature is valid.",
                        processor_name);

        return std::make_unique<JwksJwtProcessor>(processor_name, token_cache_lifetime, username_claim, groups_claim,
                                                  issuer, expected_audience, expected_typ, allow_no_expiration,
                                                  config.getString(prefix + ".claims", ""),
                                                  config.getUInt64(prefix + ".verifier_leeway", 60),
                                                  jwks_uri,
                                                  config.getUInt64(prefix + ".jwks_cache_lifetime", 3600));
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
