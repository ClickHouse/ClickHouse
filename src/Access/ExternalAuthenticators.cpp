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

auto parseLDAPServer(const Poco::Util::AbstractConfiguration & config, const String & ldap_server_name)
{
    if (ldap_server_name.empty())
        throw Exception("LDAP server name cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    LDAPServerParams params;

    const String ldap_server_config = "ldap_servers." + ldap_server_name;

    const bool has_host = config.has(ldap_server_config + ".host");
    const bool has_port = config.has(ldap_server_config + ".port");
    const bool has_auth_dn_prefix = config.has(ldap_server_config + ".auth_dn_prefix");
    const bool has_auth_dn_suffix = config.has(ldap_server_config + ".auth_dn_suffix");
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

    if (has_auth_dn_prefix)
        params.auth_dn_prefix = config.getString(ldap_server_config + ".auth_dn_prefix");

    if (has_auth_dn_suffix)
        params.auth_dn_suffix = config.getString(ldap_server_config + ".auth_dn_suffix");

    if (has_verification_cooldown)
        params.verification_cooldown = std::chrono::seconds{config.getUInt64(ldap_server_config + ".verification_cooldown")};

    if (has_enable_tls)
    {
        String enable_tls_lc_str = config.getString(ldap_server_config + ".enable_tls");
        boost::to_lower(enable_tls_lc_str);

        if (enable_tls_lc_str == "starttls")
            params.enable_tls = LDAPServerParams::TLSEnable::YES_STARTTLS;
        else if (config.getBool(ldap_server_config + ".enable_tls"))
            params.enable_tls = LDAPServerParams::TLSEnable::YES;
        else
            params.enable_tls = LDAPServerParams::TLSEnable::NO;
    }

    if (has_tls_minimum_protocol_version)
    {
        String tls_minimum_protocol_version_lc_str = config.getString(ldap_server_config + ".tls_minimum_protocol_version");
        boost::to_lower(tls_minimum_protocol_version_lc_str);

        if (tls_minimum_protocol_version_lc_str == "ssl2")
            params.tls_minimum_protocol_version = LDAPServerParams::TLSProtocolVersion::SSL2;
        else if (tls_minimum_protocol_version_lc_str == "ssl3")
            params.tls_minimum_protocol_version = LDAPServerParams::TLSProtocolVersion::SSL3;
        else if (tls_minimum_protocol_version_lc_str == "tls1.0")
            params.tls_minimum_protocol_version = LDAPServerParams::TLSProtocolVersion::TLS1_0;
        else if (tls_minimum_protocol_version_lc_str == "tls1.1")
            params.tls_minimum_protocol_version = LDAPServerParams::TLSProtocolVersion::TLS1_1;
        else if (tls_minimum_protocol_version_lc_str == "tls1.2")
            params.tls_minimum_protocol_version = LDAPServerParams::TLSProtocolVersion::TLS1_2;
        else
            throw Exception("Bad value for 'tls_minimum_protocol_version' entry, allowed values are: 'ssl2', 'ssl3', 'tls1.0', 'tls1.1', 'tls1.2'", ErrorCodes::BAD_ARGUMENTS);
    }

    if (has_tls_require_cert)
    {
        String tls_require_cert_lc_str = config.getString(ldap_server_config + ".tls_require_cert");
        boost::to_lower(tls_require_cert_lc_str);

        if (tls_require_cert_lc_str == "never")
            params.tls_require_cert = LDAPServerParams::TLSRequireCert::NEVER;
        else if (tls_require_cert_lc_str == "allow")
            params.tls_require_cert = LDAPServerParams::TLSRequireCert::ALLOW;
        else if (tls_require_cert_lc_str == "try")
            params.tls_require_cert = LDAPServerParams::TLSRequireCert::TRY;
        else if (tls_require_cert_lc_str == "demand")
            params.tls_require_cert = LDAPServerParams::TLSRequireCert::DEMAND;
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
        params.port = (params.enable_tls == LDAPServerParams::TLSEnable::YES ? 636 : 389);

    return params;
}

}

void ExternalAuthenticators::reset()
{
    std::scoped_lock lock(mutex);
    ldap_server_params.clear();
    ldap_server_caches.clear();
}

void ExternalAuthenticators::setConfiguration(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
{
    std::scoped_lock lock(mutex);

    reset();

    Poco::Util::AbstractConfiguration::Keys ldap_server_names;
    config.keys("ldap_servers", ldap_server_names);
    for (const auto & ldap_server_name : ldap_server_names)
    {
        try
        {
            ldap_server_params.insert_or_assign(ldap_server_name, parseLDAPServer(config, ldap_server_name));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse LDAP server " + backQuote(ldap_server_name));
        }
    }
}

bool ExternalAuthenticators::checkLDAPCredentials(const String & server, const String & user_name, const String & password) const
{
    std::optional<LDAPServerParams> params;
    std::size_t params_hash = 0;

    {
        std::scoped_lock lock(mutex);

        // Retrieve the server parameters.
        const auto pit = ldap_server_params.find(server);
        if (pit == ldap_server_params.end())
            throw Exception("LDAP server '" + server + "' is not configured", ErrorCodes::BAD_ARGUMENTS);

        params = pit->second;
        params->user = user_name;
        params->password = password;
        params_hash = params->getCoreHash();

        // Check the cache, but only if the caching is enabled at all.
        if (params->verification_cooldown > std::chrono::seconds{0})
        {
            const auto cit = ldap_server_caches.find(server);
            if (cit != ldap_server_caches.end())
            {
                auto & cache = cit->second;

                const auto eit = cache.find(user_name);
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
                        last_check_period <= params->verification_cooldown
                    )
                    {
                        return true;
                    }

                    // Erase the entry, if expired.
                    if (last_check_period > params->verification_cooldown)
                        cache.erase(eit);
                }

                // Erase the cache, if empty.
                if (cache.empty())
                    ldap_server_caches.erase(cit);
            }
        }
    }

    LDAPSimpleAuthClient client(params.value());
    const auto result = client.check();
    const auto current_check_timestamp = std::chrono::steady_clock::now();

    // Update the cache, but only if this is the latest check and the server is still configured in a compatible way.
    if (result)
    {
        std::scoped_lock lock(mutex);

        // If the server was removed from the config while we were checking the password, we discard the current result.
        const auto pit = ldap_server_params.find(server);
        if (pit == ldap_server_params.end())
            return false;

        auto new_params = pit->second;
        new_params.user = user_name;
        new_params.password = password;

        // If the critical server params have changed while we were checking the password, we discard the current result.
        if (params_hash != new_params.getCoreHash())
            return false;

        auto & entry = ldap_server_caches[server][user_name];
        if (entry.last_successful_authentication_timestamp < current_check_timestamp)
        {
            entry.last_successful_params_hash = params_hash;
            entry.last_successful_authentication_timestamp = current_check_timestamp;
        }
        else if (entry.last_successful_params_hash != params_hash)
        {
            // Somehow a newer check with different params/password succeeded, so the current result is obsolete and we discard it.
            return false;
        }
    }

    return result;
}

}
