#include <Access/ExternalAuthenticators.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/case_conv.hpp>


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

void parseAndAddLDAPServers(ExternalAuthenticators & external_authenticators, const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
{
    Poco::Util::AbstractConfiguration::Keys ldap_server_names;
    config.keys("ldap_servers", ldap_server_names);

    for (const auto & ldap_server_name : ldap_server_names)
    {
        try
        {
            external_authenticators.setLDAPServerParams(ldap_server_name, parseLDAPServer(config, ldap_server_name));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse LDAP server " + backQuote(ldap_server_name));
        }
    }
}

}

void ExternalAuthenticators::reset()
{
    std::scoped_lock lock(mutex);
    ldap_server_params.clear();
}

void ExternalAuthenticators::setConfig(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
{
    std::scoped_lock lock(mutex);
    reset();
    parseAndAddLDAPServers(*this, config, log);
}

void ExternalAuthenticators::setLDAPServerParams(const String & server, const LDAPServerParams & params)
{
    std::scoped_lock lock(mutex);
    ldap_server_params.erase(server);
    ldap_server_params[server] = params;
}

LDAPServerParams ExternalAuthenticators::getLDAPServerParams(const String & server) const
{
    std::scoped_lock lock(mutex);
    auto it = ldap_server_params.find(server);
    if (it == ldap_server_params.end())
        throw Exception("LDAP server '" + server + "' is not configured", ErrorCodes::BAD_ARGUMENTS);
    return it->second;
}

}
