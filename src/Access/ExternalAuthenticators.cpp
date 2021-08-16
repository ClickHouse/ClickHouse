#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/case_conv.hpp>

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
            throw Exception("Invalid value for 'scope' field of LDAP search parameters in '" + prefix +
                "' section, must be one of 'base', 'one_level', 'subtree', or 'children'", ErrorCodes::BAD_ARGUMENTS);
    }
}

void parseLDAPServer(LDAPClient::Params & params, const Poco::Util::AbstractConfiguration & config, const String & name)
{
    if (name.empty())
        throw Exception("LDAP server name cannot be empty", ErrorCodes::BAD_ARGUMENTS);

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

    if (!has_host)
        throw Exception("Missing 'host' entry", ErrorCodes::BAD_ARGUMENTS);

    params.host = config.getString(ldap_server_config + ".host");

    if (params.host.empty())
        throw Exception("Empty 'host' entry", ErrorCodes::BAD_ARGUMENTS);

    if (has_bind_dn)
    {
        if (has_auth_dn_prefix || has_auth_dn_suffix)
            throw Exception("Deprecated 'auth_dn_prefix' and 'auth_dn_suffix' entries cannot be used with 'bind_dn' entry", ErrorCodes::BAD_ARGUMENTS);

        params.bind_dn = config.getString(ldap_server_config + ".bind_dn");
    }
    else if (has_auth_dn_prefix || has_auth_dn_suffix)
    {
        const auto auth_dn_prefix = config.getString(ldap_server_config + ".auth_dn_prefix");
        const auto auth_dn_suffix = config.getString(ldap_server_config + ".auth_dn_suffix");
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
            params.enable_tls = LDAPClient::Params::TLSEnable::YES; //-V1048
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
            params.tls_minimum_protocol_version = LDAPClient::Params::TLSProtocolVersion::TLS1_2; //-V1048
        else
            throw Exception("Bad value for 'tls_minimum_protocol_version' entry, allowed values are: 'ssl2', 'ssl3', 'tls1.0', 'tls1.1', 'tls1.2'", ErrorCodes::BAD_ARGUMENTS);
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
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::DEMAND; //-V1048
        else
            throw Exception("Bad value for 'tls_require_cert' entry, allowed values are: 'never', 'allow', 'try', 'demand'", ErrorCodes::BAD_ARGUMENTS);
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
        const auto port = config.getInt64(ldap_server_config + ".port");
        if (port < 0 || port > 65535)
            throw Exception("Bad value for 'port' entry", ErrorCodes::BAD_ARGUMENTS);

        params.port = port;
    }
    else
        params.port = (params.enable_tls == LDAPClient::Params::TLSEnable::YES ? 636 : 389);
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
        throw Exception("Realm and principal name cannot be specified simultaneously", ErrorCodes::BAD_ARGUMENTS);

    if (reealm_key_count > 1)
        throw Exception("Multiple realm sections are not allowed", ErrorCodes::BAD_ARGUMENTS);

    if (principal_keys_count > 1)
        throw Exception("Multiple principal sections are not allowed", ErrorCodes::BAD_ARGUMENTS);

    params.realm = config.getString("kerberos.realm", "");
    params.principal = config.getString("kerberos.principal", "");
}

}

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    parseLDAPSearchParams(params, config, prefix);

    const bool has_prefix = config.has(prefix + ".prefix");

    if (has_prefix)
        params.prefix = config.getString(prefix + ".prefix");
}

void ExternalAuthenticators::reset()
{
    std::scoped_lock lock(mutex);
    ldap_client_params_blueprint.clear();
    ldap_caches.clear();
    kerberos_params.reset();
}

void ExternalAuthenticators::setConfiguration(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
{
    std::scoped_lock lock(mutex);
    reset();

    Poco::Util::AbstractConfiguration::Keys all_keys;
    config.keys("", all_keys);

    std::size_t ldap_servers_key_count = 0;
    std::size_t kerberos_keys_count = 0;

    for (auto key : all_keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            key.resize(bracket_pos);

        boost::algorithm::to_lower(key);

        ldap_servers_key_count += (key == "ldap_servers");
        kerberos_keys_count += (key == "kerberos");
    }

    if (ldap_servers_key_count > 1)
        throw Exception("Multiple ldap_servers sections are not allowed", ErrorCodes::BAD_ARGUMENTS);

    if (kerberos_keys_count > 1)
        throw Exception("Multiple kerberos sections are not allowed", ErrorCodes::BAD_ARGUMENTS);

    Poco::Util::AbstractConfiguration::Keys ldap_server_names;
    config.keys("ldap_servers", ldap_server_names);
    for (const auto & ldap_server_name : ldap_server_names)
    {
        try
        {
            ldap_client_params_blueprint.erase(ldap_server_name);
            parseLDAPServer(ldap_client_params_blueprint.emplace(ldap_server_name, LDAPClient::Params{}).first->second, config, ldap_server_name);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse LDAP server " + backQuote(ldap_server_name));
        }
    }

    try
    {
        if (kerberos_keys_count > 0)
            parseKerberosParams(kerberos_params.emplace(), config);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Could not parse Kerberos section");
    }
}

bool ExternalAuthenticators::checkLDAPCredentials(const String & server, const BasicCredentials & credentials,
    const LDAPClient::RoleSearchParamsList * role_search_params, LDAPClient::SearchResultsList * role_search_results) const
{
    std::optional<LDAPClient::Params> params;
    std::size_t params_hash = 0;

    {
        std::scoped_lock lock(mutex);

        // Retrieve the server parameters.
        const auto pit = ldap_client_params_blueprint.find(server);
        if (pit == ldap_client_params_blueprint.end())
            throw Exception("LDAP server '" + server + "' is not configured", ErrorCodes::BAD_ARGUMENTS);

        params = pit->second;
        params->user = credentials.getUserName();
        params->password = credentials.getPassword();

        params->combineCoreHash(params_hash);
        if (role_search_params)
        {
            for (const auto & params_instance : *role_search_params)
            {
                params_instance.combineHash(params_hash);
            }
        }

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
        std::scoped_lock lock(mutex);

        // If the server was removed from the config while we were checking the password, we discard the current result.
        const auto pit = ldap_client_params_blueprint.find(server);
        if (pit == ldap_client_params_blueprint.end())
            return false;

        auto new_params = pit->second;
        new_params.user = credentials.getUserName();
        new_params.password = credentials.getPassword();

        std::size_t new_params_hash = 0;
        new_params.combineCoreHash(new_params_hash);
        if (role_search_params)
        {
            for (const auto & params_instance : *role_search_params)
            {
                params_instance.combineHash(new_params_hash);
            }
        }

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
    std::scoped_lock lock(mutex);

    if (!kerberos_params.has_value())
        throw Exception("Kerberos is not enabled", ErrorCodes::BAD_ARGUMENTS);

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
    std::scoped_lock lock(mutex);

    if (!kerberos_params.has_value())
        throw Exception("Kerberos is not enabled", ErrorCodes::BAD_ARGUMENTS);

    return kerberos_params.value();
}

}
