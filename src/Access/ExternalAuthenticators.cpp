#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/SettingsAuthResponseParser.h>
#include <Access/resolveSetting.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ClientInfo.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <map>
#include <memory>
#include <optional>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void parseLDAPSearchParams(LDAPClient::SearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    const bool has_base_dn = config.has(prefix + ".base_dn");
    const bool has_search_filter = config.has(prefix + ".search_filter");
    const bool has_attribute = config.has(prefix + ".attribute");
    const bool has_scope = config.has(prefix + ".scope");

    if (has_base_dn)
        params.base_dn = config.getString(prefix + ".base_dn");

    if (has_search_filter)
        params.search_filter = config.getString(prefix + ".search_filter");

    if (has_attribute)
        params.attribute = config.getString(prefix + ".attribute");

    if (has_scope)
    {
        auto scope = config.getString(prefix + ".scope");
        boost::algorithm::to_lower(scope);

        if (scope == "base")           params.scope = LDAPClient::SearchParams::Scope::BASE;
        else if (scope == "one_level") params.scope = LDAPClient::SearchParams::Scope::ONE_LEVEL;
        else if (scope == "subtree")   params.scope = LDAPClient::SearchParams::Scope::SUBTREE;
        else if (scope == "children")  params.scope = LDAPClient::SearchParams::Scope::CHILDREN;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid value for 'scope' field of LDAP search parameters "
                            "in '{}' section, must be one of 'base', 'one_level', 'subtree', or 'children'", prefix);
    }
}

void parseLDAPServer(LDAPClient::Params & params, const Poco::Util::AbstractConfiguration & config, const String & name)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP server name cannot be empty");

    const String ldap_server_config = "ldap_servers." + name;

    const bool has_host = config.has(ldap_server_config + ".host");
    const bool has_port = config.has(ldap_server_config + ".port");
    const bool has_bind_dn = config.has(ldap_server_config + ".bind_dn");
    const bool has_auth_dn_prefix = config.has(ldap_server_config + ".auth_dn_prefix");
    const bool has_auth_dn_suffix = config.has(ldap_server_config + ".auth_dn_suffix");
    const bool has_user_dn_detection = config.has(ldap_server_config + ".user_dn_detection");
    const bool has_verification_cooldown = config.has(ldap_server_config + ".verification_cooldown");
    const bool has_enable_tls = config.has(ldap_server_config + ".enable_tls");
    const bool has_tls_minimum_protocol_version = config.has(ldap_server_config + ".tls_minimum_protocol_version");
    const bool has_tls_require_cert = config.has(ldap_server_config + ".tls_require_cert");
    const bool has_tls_cert_file = config.has(ldap_server_config + ".tls_cert_file");
    const bool has_tls_key_file = config.has(ldap_server_config + ".tls_key_file");
    const bool has_tls_ca_cert_file = config.has(ldap_server_config + ".tls_ca_cert_file");
    const bool has_tls_ca_cert_dir = config.has(ldap_server_config + ".tls_ca_cert_dir");
    const bool has_tls_cipher_suite = config.has(ldap_server_config + ".tls_cipher_suite");
    const bool has_search_limit = config.has(ldap_server_config + ".search_limit");
    const bool has_follow_referrals = config.has(ldap_server_config + ".follow_referrals");

    if (!has_host)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'host' entry");

    params.host = config.getString(ldap_server_config + ".host");

    if (params.host.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'host' entry");

    if (has_bind_dn)
    {
        if (has_auth_dn_prefix || has_auth_dn_suffix)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deprecated 'auth_dn_prefix' and 'auth_dn_suffix' entries cannot be used with 'bind_dn' entry");

        params.bind_dn = config.getString(ldap_server_config + ".bind_dn");
    }
    else if (has_auth_dn_prefix || has_auth_dn_suffix)
    {
        std::string auth_dn_prefix = config.getString(ldap_server_config + ".auth_dn_prefix");
        std::string auth_dn_suffix = config.getString(ldap_server_config + ".auth_dn_suffix");
        params.bind_dn = auth_dn_prefix + "{user_name}" + auth_dn_suffix;
    }

    if (has_user_dn_detection)
    {
        if (!params.user_dn_detection)
        {
            params.user_dn_detection.emplace();
            params.user_dn_detection->attribute = "dn";
        }

        parseLDAPSearchParams(*params.user_dn_detection, config, ldap_server_config + ".user_dn_detection");
    }

    if (has_verification_cooldown)
        params.verification_cooldown = std::chrono::seconds{config.getUInt64(ldap_server_config + ".verification_cooldown")};

    if (has_enable_tls)
    {
        String enable_tls_lc_str = config.getString(ldap_server_config + ".enable_tls");
        boost::to_lower(enable_tls_lc_str);

        if (enable_tls_lc_str == "starttls")
            params.enable_tls = LDAPClient::Params::TLSEnable::YES_STARTTLS;
        else if (config.getBool(ldap_server_config + ".enable_tls"))
            params.enable_tls = LDAPClient::Params::TLSEnable::YES;
        else
            params.enable_tls = LDAPClient::Params::TLSEnable::NO;
    }

    if (has_tls_minimum_protocol_version)
    {
        String tls_minimum_protocol_version_lc_str = config.getString(ldap_server_config + ".tls_minimum_protocol_version");
        boost::to_lower(tls_minimum_protocol_version_lc_str);

        if (tls_minimum_protocol_version_lc_str == "ssl2")
            params.tls_minimum_protocol_version = LDAPClient::Params::TLSProtocolVersion::SSL2;
        else if (tls_minimum_protocol_version_lc_str == "ssl3")
            params.tls_minimum_protocol_version = LDAPClient::Params::TLSProtocolVersion::SSL3;
        else if (tls_minimum_protocol_version_lc_str == "tls1.0")
            params.tls_minimum_protocol_version = LDAPClient::Params::TLSProtocolVersion::TLS1_0;
        else if (tls_minimum_protocol_version_lc_str == "tls1.1")
            params.tls_minimum_protocol_version = LDAPClient::Params::TLSProtocolVersion::TLS1_1;
        else if (tls_minimum_protocol_version_lc_str == "tls1.2")
            params.tls_minimum_protocol_version = LDAPClient::Params::TLSProtocolVersion::TLS1_2;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Bad value for 'tls_minimum_protocol_version' entry, allowed values are: "
                            "'ssl2', 'ssl3', 'tls1.0', 'tls1.1', 'tls1.2'");
    }

    if (has_tls_require_cert)
    {
        String tls_require_cert_lc_str = config.getString(ldap_server_config + ".tls_require_cert");
        boost::to_lower(tls_require_cert_lc_str);

        if (tls_require_cert_lc_str == "never")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::NEVER;
        else if (tls_require_cert_lc_str == "allow")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::ALLOW;
        else if (tls_require_cert_lc_str == "try")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::TRY;
        else if (tls_require_cert_lc_str == "demand")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::DEMAND;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Bad value for 'tls_require_cert' entry, allowed values are: "
                            "'never', 'allow', 'try', 'demand'");
    }

    if (has_tls_cert_file)
        params.tls_cert_file = config.getString(ldap_server_config + ".tls_cert_file");

    if (has_tls_key_file)
        params.tls_key_file = config.getString(ldap_server_config + ".tls_key_file");

    if (has_tls_ca_cert_file)
        params.tls_ca_cert_file = config.getString(ldap_server_config + ".tls_ca_cert_file");

    if (has_tls_ca_cert_dir)
        params.tls_ca_cert_dir = config.getString(ldap_server_config + ".tls_ca_cert_dir");

    if (has_tls_cipher_suite)
        params.tls_cipher_suite = config.getString(ldap_server_config + ".tls_cipher_suite");

    if (has_port)
    {
        UInt32 port = config.getUInt(ldap_server_config + ".port");
        if (port > 65535)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad value for 'port' entry");

        params.port = static_cast<UInt16>(port);
    }
    else
        params.port = (params.enable_tls == LDAPClient::Params::TLSEnable::YES ? 636 : 389);

    if (has_search_limit)
        params.search_limit = static_cast<UInt32>(config.getUInt64(ldap_server_config + ".search_limit"));

    if (has_follow_referrals)
        params.follow_referrals = config.getBool(ldap_server_config + ".follow_referrals");
}

void parseKerberosParams(GSSAcceptorContext::Params & params, const Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("kerberos", keys);

    std::size_t reealm_key_count = 0;
    std::size_t principal_keys_count = 0;

    for (auto key : keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            key.resize(bracket_pos);

        boost::algorithm::to_lower(key);

        reealm_key_count += (key == "realm");
        principal_keys_count += (key == "principal");
    }

    if (reealm_key_count > 0 && principal_keys_count > 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Realm and principal name cannot be specified simultaneously");

    if (reealm_key_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple realm sections are not allowed");

    if (principal_keys_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple principal sections are not allowed");

    params.realm = config.getString("kerberos.realm", "");
    params.principal = config.getString("kerberos.principal", "");
    params.keytab = config.getString("kerberos.keytab", "");
}

HTTPAuthClientParams parseHTTPAuthParams(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    HTTPAuthClientParams http_auth_params;

    http_auth_params.uri = config.getString(prefix + ".uri");

    size_t connection_timeout_ms = config.getInt(prefix + ".connection_timeout_ms", 1000);
    size_t receive_timeout_ms = config.getInt(prefix + ".receive_timeout_ms", 1000);
    size_t send_timeout_ms = config.getInt(prefix + ".send_timeout_ms", 1000);
    http_auth_params.timeouts = ConnectionTimeouts()
        .withConnectionTimeout(Poco::Timespan(connection_timeout_ms * 1000))
        .withReceiveTimeout(Poco::Timespan(receive_timeout_ms * 1000))
        .withSendTimeout(Poco::Timespan(send_timeout_ms * 1000));

    http_auth_params.max_tries = config.getInt(prefix + ".max_tries", 3);
    http_auth_params.retry_initial_backoff_ms = config.getInt(prefix + ".retry_initial_backoff_ms", 50);
    http_auth_params.retry_max_backoff_ms = config.getInt(prefix + ".retry_max_backoff_ms", 1000);

    Strings forward_headers;
    config.keys(prefix + ".forward_headers", forward_headers);
    for (const auto & header : forward_headers)
    {
        String name = config.getString(prefix + ".forward_headers." + header);
        http_auth_params.forward_headers.push_back(name);
    }

    return http_auth_params;
}
}

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    parseLDAPSearchParams(params, config, prefix);

    const bool has_prefix = config.has(prefix + ".prefix");

    if (has_prefix)
        params.prefix = config.getString(prefix + ".prefix");
}

void ExternalAuthenticators::resetImpl()
{
    ldap_client_params_blueprint.clear();
    ldap_caches.clear();
    kerberos_params.reset();
    token_processors.clear();
    access_token_to_username_cache.clear();
    username_to_access_token_cache.clear();
}

void ExternalAuthenticators::reset()
{
    std::lock_guard lock(mutex);
    resetImpl();
}

/// Parse all token processors as an all-or-nothing operation.
///
/// Throws if ANY processor fails to parse. The caller is expected to react by
/// disabling token authentication for this configuration cycle (fail-closed).
void parseTokenProcessors(std::map<String, std::shared_ptr<ITokenProcessor>> & token_processors,
                        const Poco::Util::AbstractConfiguration & config,
                        const String & token_processors_config,
                        LoggerPtr log)
{
    Poco::Util::AbstractConfiguration::Keys token_processors_keys;
    config.keys(token_processors_config, token_processors_keys);

    /// Build into a local map first so the live set is never observed in a partially-constructed state.
    /// Ordered so the auto-discovery iteration order in `checkTokenCredentials` is stable.
    std::map<String, std::shared_ptr<ITokenProcessor>> parsed;

    for (const auto & processor : token_processors_keys)
    {
        String prefix = fmt::format("{}.{}", token_processors_config, processor);
        try
        {
            parsed[processor] = ITokenProcessor::parseTokenProcessor(config, prefix, processor);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse token processor " + backQuote(processor));
            /// Re-throw so the caller fails.
            throw;
        }
    }

    token_processors = std::move(parsed);
}

bool ExternalAuthenticators::isTokenAuthEnabled() const
{
    std::lock_guard lock(mutex);
    return token_auth_enabled;
}

bool ExternalAuthenticators::hasTokenProcessor(const String & name) const
{
    std::lock_guard lock(mutex);
    if (!token_auth_enabled)
        return false;
    if (name.empty())
        return true;
    return token_processors.contains(name);
}

void ExternalAuthenticators::setConfiguration(const Poco::Util::AbstractConfiguration & config, LoggerPtr log, bool token_auth_enabled_)
{
    std::lock_guard lock(mutex);
    resetImpl();
    token_auth_enabled = token_auth_enabled_;

    Poco::Util::AbstractConfiguration::Keys all_keys;
    config.keys("", all_keys);

    std::size_t ldap_servers_key_count = 0;
    std::size_t kerberos_keys_count = 0;
    std::size_t http_auth_server_keys_count = 0;
    std::size_t jwt_validators_count = 0;
    std::size_t token_processors_count = 0;

    const String http_auth_servers_config = "http_authentication_servers";
    const String jwt_validators_config = "jwt_validators";
    const String token_processors_config = "token_processors";

    for (auto key : all_keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            key.resize(bracket_pos);

        boost::algorithm::to_lower(key);

        ldap_servers_key_count += (key == "ldap_servers");
        kerberos_keys_count += (key == "kerberos");
        http_auth_server_keys_count += (key == http_auth_servers_config);
        jwt_validators_count += (key == jwt_validators_config);
        token_processors_count += (key == token_processors_config);
    }

    if (ldap_servers_key_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple ldap_servers sections are not allowed");

    if (kerberos_keys_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple kerberos sections are not allowed");

    if (http_auth_server_keys_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple http_authentication_servers sections are not allowed");

    if (jwt_validators_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple {} sections are not allowed", jwt_validators_config);

    if (token_processors_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple {} sections are not allowed", token_processors_config);

    Poco::Util::AbstractConfiguration::Keys http_auth_server_names;
    config.keys(http_auth_servers_config, http_auth_server_names);
    http_auth_servers.clear();
    for (const auto & http_auth_server_name : http_auth_server_names)
    {
        String prefix = fmt::format("{}.{}", http_auth_servers_config, http_auth_server_name);
        try
        {
            http_auth_servers[http_auth_server_name] = parseHTTPAuthParams(config, prefix);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse HTTP auth server" + backQuote(http_auth_server_name));
        }
    }

    Poco::Util::AbstractConfiguration::Keys ldap_server_names;
    config.keys("ldap_servers", ldap_server_names);
    ldap_client_params_blueprint.clear();
    for (auto ldap_server_name : ldap_server_names)
    {
        try
        {
            const auto bracket_pos = ldap_server_name.find('[');
            if (bracket_pos != std::string::npos)
                ldap_server_name.resize(bracket_pos);

            if (ldap_client_params_blueprint.contains(ldap_server_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple LDAP servers with the same name are not allowed");

            LDAPClient::Params ldap_client_params_tmp;
            parseLDAPServer(ldap_client_params_tmp, config, ldap_server_name);
            ldap_client_params_blueprint.emplace(std::move(ldap_server_name), std::move(ldap_client_params_tmp));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse LDAP server " + backQuote(ldap_server_name));
        }
    }

    kerberos_params.reset();
    try
    {
        if (kerberos_keys_count > 0)
        {
            GSSAcceptorContext::Params kerberos_params_tmp;
            parseKerberosParams(kerberos_params_tmp, config);
            kerberos_params = std::move(kerberos_params_tmp);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Could not parse Kerberos section");
    }

    if (token_auth_enabled)
    {
        try
        {
            parseTokenProcessors(token_processors, config, token_processors_config, log);
        }
        catch (...)
        {
            /// Fail closed: if any token processor failed to parse, refuse to
            /// activate token auth at all for this config cycle.
            tryLogCurrentException(log,
                "One or more token processors failed to parse; "
                "disabling token authentication entirely until the configuration is fixed");
            token_processors.clear();
            token_auth_enabled = false;
        }
    }
    else
        LOG_INFO(log, "Token authentication is disabled, skipping token processors configuration");
}

static UInt128 computeParamsHash(const LDAPClient::Params & params, const LDAPClient::RoleSearchParamsList * role_search_params)
{
    SipHash hash;
    params.updateHash(hash);
    if (role_search_params)
    {
        for (const auto & params_instance : *role_search_params)
        {
            params_instance.updateHash(hash);
        }
    }

    return hash.get128();
}

bool ExternalAuthenticators::checkLDAPCredentials(const String & server, const BasicCredentials & credentials,
    const LDAPClient::RoleSearchParamsList * role_search_params, LDAPClient::SearchResultsList * role_search_results) const
{
    std::optional<LDAPClient::Params> params;
    UInt128 params_hash = 0;

    {
        std::lock_guard lock(mutex);

        // Retrieve the server parameters.
        const auto pit = ldap_client_params_blueprint.find(server);
        if (pit == ldap_client_params_blueprint.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP server '{}' is not configured", server);

        params = pit->second;
        params->user = credentials.getUserName();
        params->password = credentials.getPassword();

        params_hash = computeParamsHash(*params, role_search_params);

        // Check the cache, but only if the caching is enabled at all.
        if (params->verification_cooldown > std::chrono::seconds{0})
        {
            const auto cit = ldap_caches.find(server);
            if (cit != ldap_caches.end())
            {
                auto & cache = cit->second;

                const auto eit = cache.find(credentials.getUserName());
                if (eit != cache.end())
                {
                    const auto & entry = eit->second;
                    const auto last_check_period = std::chrono::steady_clock::now() - entry.last_successful_authentication_timestamp;

                    if (
                        // Forbid the initial values explicitly.
                        entry.last_successful_params_hash != 0 &&
                        entry.last_successful_authentication_timestamp != std::chrono::steady_clock::time_point{} &&

                        // Check if we can safely "reuse" the result of the previous successful password verification.
                        entry.last_successful_params_hash == params_hash &&
                        last_check_period >= std::chrono::seconds{0} &&
                        last_check_period <= params->verification_cooldown &&

                        // Ensure that search_params are compatible.
                        (
                            role_search_params == nullptr ?
                            entry.last_successful_role_search_results.empty() :
                            role_search_params->size() == entry.last_successful_role_search_results.size()
                        )
                    )
                    {
                        if (role_search_results)
                            *role_search_results = entry.last_successful_role_search_results;

                        return true;
                    }

                    // Erase the entry, if expired.
                    if (last_check_period > params->verification_cooldown)
                        cache.erase(eit);
                }

                // Erase the cache, if empty.
                if (cache.empty())
                    ldap_caches.erase(cit);
            }
        }
    }

    LDAPSimpleAuthClient client(params.value());
    const auto result = client.authenticate(role_search_params, role_search_results);
    const auto current_check_timestamp = std::chrono::steady_clock::now();

    // Update the cache, but only if this is the latest check and the server is still configured in a compatible way.
    if (result)
    {
        std::lock_guard lock(mutex);

        // If the server was removed from the config while we were checking the password, we discard the current result.
        const auto pit = ldap_client_params_blueprint.find(server);
        if (pit == ldap_client_params_blueprint.end())
            return false;

        auto new_params = pit->second;
        new_params.user = credentials.getUserName();
        new_params.password = credentials.getPassword();

        const UInt128 new_params_hash = computeParamsHash(new_params, role_search_params);

        // If the critical server params have changed while we were checking the password, we discard the current result.
        if (params_hash != new_params_hash)
            return false;

        auto & entry = ldap_caches[server][credentials.getUserName()];
        if (entry.last_successful_authentication_timestamp < current_check_timestamp)
        {
            entry.last_successful_params_hash = params_hash;
            entry.last_successful_authentication_timestamp = current_check_timestamp;

            if (role_search_results)
                entry.last_successful_role_search_results = *role_search_results;
            else
                entry.last_successful_role_search_results.clear();
        }
        else if (
            entry.last_successful_params_hash != params_hash ||
            (
                role_search_params == nullptr ?
                !entry.last_successful_role_search_results.empty() :
                role_search_params->size() != entry.last_successful_role_search_results.size()
            )
        )
        {
            // Somehow a newer check with different params/password succeeded, so the current result is obsolete and we discard it.
            return false;
        }
    }

    return result;
}

bool ExternalAuthenticators::checkKerberosCredentials(const String & realm, const GSSAcceptorContext & credentials) const
{
    std::lock_guard lock(mutex);

    if (!kerberos_params.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kerberos is not enabled");

    if (!credentials.isReady())
        return false;

    if (credentials.isFailed())
        return false;

    if (!realm.empty() && realm != credentials.getRealm())
        return false;

    return true;
}

GSSAcceptorContext::Params ExternalAuthenticators::getKerberosParams() const
{
    std::lock_guard lock(mutex);

    if (!kerberos_params.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kerberos is not enabled");

    return kerberos_params.value();
}

HTTPAuthClientParams ExternalAuthenticators::getHTTPAuthenticationParams(const String & server) const
{
    std::lock_guard lock{mutex};

    const auto it = http_auth_servers.find(server);
    if (it == http_auth_servers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP server '{}' is not configured", server);
    return it->second;
}

bool ExternalAuthenticators::checkCredentialsAgainstProcessor(const ITokenProcessor & processor,
                                                              TokenCredentials & credentials) const
{
    if (!processor.resolveAndValidate(credentials))
    {
        LOG_TRACE(getLogger("AccessTokenAuthentication"), "Failed authentication with access token by {}", processor.getProcessorName());
        return false;
    }

    /// Clamp the credentials' expires_at to the processor's cache lifetime so
    /// upper layers (notably `Session`) bind their lifetime to whichever is
    /// shorter -- the token's own expiry or the operator-configured TTL. This
    /// is a *post-validation finalization* of the credentials, not a cache
    /// write; the actual token-cache entry is written by `primeTokenCache`,
    /// and only after any per-user `jwt_claims` policy has also accepted the
    /// token (see `checkTokenCredentials`).
    auto default_expiration_ts = std::chrono::system_clock::now()
                                 + std::chrono::seconds(processor.getTokenCacheLifetime());

    if (credentials.getExpiresAt().has_value())
    {
        if (credentials.getExpiresAt().value() >= default_expiration_ts)
        {
            LOG_TRACE(getLogger("AccessTokenAuthentication"), "Token for user {} expires after default cache lifetime; using default TTL by {}", credentials.getUserName(), processor.getProcessorName());
            credentials.setExpiresAt(default_expiration_ts);
        }
    }
    else
    {
        credentials.setExpiresAt(default_expiration_ts);
    }

    LOG_DEBUG(getLogger("AccessTokenAuthentication"), "Authenticated user {} with access token by {}", quoteString(credentials.getUserName()), processor.getProcessorName());
    return true;
}

void ExternalAuthenticators::primeTokenCache(const ITokenProcessor & processor,
                                             const TokenCredentials & credentials) const
{
    /// Build a cache entry from the credentials state that
    /// `checkCredentialsAgainstProcessor` finalized. The caller is responsible
    /// for invoking this only after both processor validation AND the per-user
    /// `jwt_claims` policy have accepted the token -- caching before claims
    /// have been evaluated would let later unconstrained lookups (e.g. the
    /// HTTP/TCP pre-user-lookup call which passes empty `jwt_claims`) hit a
    /// cache entry that never actually satisfied the user's policy.
    TokenCacheEntry cache_entry;
    cache_entry.user_name = credentials.getUserName();
    cache_entry.external_roles = credentials.getGroups();
    cache_entry.processor_name = processor.getProcessorName();
    cache_entry.expires_at = credentials.getExpiresAt().value_or(
        std::chrono::system_clock::now() + std::chrono::seconds(processor.getTokenCacheLifetime()));

    /// If the same token already has a forward entry that maps to a DIFFERENT
    /// user_name, clean up the stale reverse entry for that other user before
    /// we overwrite the forward entry. This happens when two processors extract
    /// different `username_claim` values from the same token (e.g. processor X
    /// uses `sub`, processor Y uses `email`): without this, the rotation step
    /// below would not see the old user's entry in the reverse map and the
    /// bi-map would diverge -- forward saying token -> new_user while a stale
    /// reverse says old_user -> token, surfacing later as a dangling reverse
    /// pointer that breaks the single-token-per-user invariant.
    auto existing_forward = access_token_to_username_cache.find(credentials.getToken());
    if (existing_forward != access_token_to_username_cache.end()
        && existing_forward->second.user_name != cache_entry.user_name)
    {
        auto stale_reverse = username_to_access_token_cache.find(existing_forward->second.user_name);
        if (stale_reverse != username_to_access_token_cache.end()
            && stale_reverse->second == credentials.getToken())
            username_to_access_token_cache.erase(stale_reverse);
    }

    /// If a previous entry exists for the same user under a different token,
    /// drop it -- the user has rotated tokens and the old one is now stale.
    auto old_token_iter = username_to_access_token_cache.find(cache_entry.user_name);
    if (old_token_iter != username_to_access_token_cache.end())
    {
        access_token_to_username_cache.erase(old_token_iter->second);
        username_to_access_token_cache.erase(old_token_iter);
    }

    access_token_to_username_cache[credentials.getToken()] = cache_entry;
    username_to_access_token_cache[cache_entry.user_name] = credentials.getToken();
    LOG_TRACE(getLogger("AccessTokenAuthentication"), "Cache entry for user {} added", quoteString(cache_entry.user_name));
}

bool ExternalAuthenticators::checkTokenCredentials(const TokenCredentials & credentials,
                                                   const String & processor_name,
                                                   const String & jwt_claims,
                                                   bool prime_cache_on_success) const
{
    /// Per-user claims restriction is binding: when a user is configured with `jwt_claims`,
    /// authentication is only allowed via processors that can actually evaluate those claims
    /// (i.e. JWT processors). If the resolving processor cannot enforce the restriction we
    /// must deny -- silently treating it as "no restriction" would let an opaque/access-token
    /// processor authenticate a token that fails the user's per-user policy.
    auto check_claims_if_required = [&](const ITokenProcessor & processor) -> bool
    {
        if (jwt_claims.empty())
            return true;
        if (!processor.supportsJwtClaimsRestriction())
        {
            LOG_TRACE(getLogger("AccessTokenAuthentication"),
                      "Processor {} does not support per-user JWT claims restriction; "
                      "denying authentication that requires claims to be checked",
                      processor.getProcessorName());
            return false;
        }
        return processor.checkClaims(credentials, jwt_claims);
    };

    /// Snapshot the processor set under the mutex, then run the expensive
    /// crypto verify WITHOUT the mutex (M-20). `shared_ptr` keeps each
    /// processor alive even if a config reload swaps `token_processors` in
    /// the middle of validation. Cache lookup stays under the mutex.
    std::map<String, std::shared_ptr<ITokenProcessor>> processors_snapshot;

    {
        std::lock_guard lock{mutex};

        if (!token_auth_enabled)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Token authentication is disabled");

        if (token_processors.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Token authentication is not configured");

        /// lookup token in local cache if not expired.
        auto cached_entry_iter = access_token_to_username_cache.find(credentials.getToken());
        if (cached_entry_iter != access_token_to_username_cache.end())
        {
            if (cached_entry_iter->second.expires_at <= std::chrono::system_clock::now()) // Token found in cache, but already outdated -- need to remove it.
            {
                const auto expired_user_name = cached_entry_iter->second.user_name;
                const auto expired_token = cached_entry_iter->first;
                LOG_TRACE(getLogger("AccessTokenAuthentication"), "Cache entry for user {} expired, removing", quoteString(expired_user_name));
                access_token_to_username_cache.erase(cached_entry_iter);

                /// Only unlink the reverse mapping if it currently points at the token
                /// we just evicted. The bi-map invariant is maintained by
                /// `primeTokenCache`, but if a reverse entry is somehow stale (or if a
                /// concurrent rotation under the same mutex hold has already pointed
                /// the user's reverse mapping at a fresh, still-valid token), erasing
                /// blindly here would unlink that fresh token's reverse entry --
                /// silently breaking the single-token-per-user invariant and extending
                /// the stale token's effective retention.
                auto reverse_it = username_to_access_token_cache.find(expired_user_name);
                if (reverse_it != username_to_access_token_cache.end() && reverse_it->second == expired_token)
                    username_to_access_token_cache.erase(reverse_it);
            }
            /// Enforce the per-user processor pin even on cache hit. A cache entry produced by
            /// processor A must NOT be used to satisfy an authentication request that is pinned
            /// to a different processor B.When the caller did not pin a processor (processor_name is
            /// empty) any cached entry is acceptable.
            else if (processor_name.empty() || processor_name == cached_entry_iter->second.processor_name)
            {
                /// Evaluate per-user claims FIRST, before mutating the outer
                /// `TokenCredentials`. The `const_cast`-ed `setUserName`/`setGroups`/
                /// `setExpiresAt` writes below would otherwise leak the cached
                /// identity into the caller's credentials object even on rejection.
                if (!jwt_claims.empty())
                {
                    const auto it = token_processors.find(cached_entry_iter->second.processor_name);
                    if (it == token_processors.end() || !check_claims_if_required(*it->second))
                        return false;
                }

                const auto & user_data = cached_entry_iter->second;
                const_cast<TokenCredentials &>(credentials).setUserName(user_data.user_name);
                const_cast<TokenCredentials &>(credentials).setGroups(user_data.external_roles);
                const_cast<TokenCredentials &>(credentials).setExpiresAt(user_data.expires_at);
                LOG_TRACE(getLogger("AccessTokenAuthentication"), "Cache entry for user {} found, using it to authenticate", quoteString(user_data.user_name));
                return true;
            }
            else
            {
                LOG_TRACE(getLogger("AccessTokenAuthentication"),
                          "Cached token entry was produced by processor {}, but authentication is pinned to {}; "
                          "ignoring cache and re-authenticating via the pinned processor",
                          cached_entry_iter->second.processor_name, processor_name);
            }
        }

        processors_snapshot = token_processors;
    }

    /// Validation path runs WITHOUT the mutex. RSA/ECDSA verifies and any
    /// expensive claim matching no longer serialize the auth subsystem.
    auto try_processor = [&](const std::shared_ptr<ITokenProcessor> & proc) -> std::optional<bool>
    {
        if (!checkCredentialsAgainstProcessor(*proc, const_cast<TokenCredentials &>(credentials)))
            return std::nullopt;
        if (!check_claims_if_required(*proc))
            return false;
        if (prime_cache_on_success)
        {
            std::lock_guard lock{mutex};
            primeTokenCache(*proc, credentials);
        }
        return true;
    };

    if (processor_name.empty())
    {
        for (const auto & [name, proc] : processors_snapshot)
        {
            if (!jwt_claims.empty() && !proc->supportsJwtClaimsRestriction())
            {
                LOG_TRACE(getLogger("AccessTokenAuthentication"),
                          "Skipping processor {} during auto-discovery: it cannot enforce per-user JWT claims",
                          proc->getProcessorName());
                continue;
            }
            if (auto result = try_processor(proc); result.has_value())
                return *result;
        }
    }
    else
    {
        const auto it = processors_snapshot.find(processor_name);
        if (it == processors_snapshot.end())
            return false;
        if (!jwt_claims.empty() && !it->second->supportsJwtClaimsRestriction())
        {
            LOG_TRACE(getLogger("AccessTokenAuthentication"),
                      "Pinned processor {} cannot enforce per-user JWT claims; denying authentication",
                      it->second->getProcessorName());
            return false;
        }
        if (auto result = try_processor(it->second); result.has_value())
            return *result;
    }

    return false;
}

bool ExternalAuthenticators::checkHTTPBasicCredentials(
    const String & server, const BasicCredentials & credentials, const ClientInfo & client_info, SettingsChanges & settings) const
{
    auto params = getHTTPAuthenticationParams(server);
    HTTPBasicAuthClient<SettingsAuthResponseParser> client(params);

    auto [is_ok, settings_from_auth_server] = client.authenticate(credentials.getUserName(), credentials.getPassword(), client_info.http_headers);

    if (is_ok)
        std::ranges::move(settings_from_auth_server, std::back_inserter(settings));

    return is_ok;
}
}
