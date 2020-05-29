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
    LDAPServerParams params;

    const String ldap_server_config = "ldap_servers." + ldap_server_name;

    const bool has_host = config.has(ldap_server_config + ".host");
    const bool has_port = config.has(ldap_server_config + ".port");
    const bool has_auth_dn_prefix = config.has(ldap_server_config + ".auth_dn_prefix");
    const bool has_auth_dn_suffix = config.has(ldap_server_config + ".auth_dn_suffix");
    const bool has_enable_tls = config.has(ldap_server_config + ".enable_tls");
    const bool has_tls_cert_verify = config.has(ldap_server_config + ".tls_cert_verify");
    const bool has_ca_cert_dir = config.has(ldap_server_config + ".ca_cert_dir");
    const bool has_ca_cert_file = config.has(ldap_server_config + ".ca_cert_file");

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

    if (has_tls_cert_verify)
    {
        String tls_cert_verify_lc_str = config.getString(ldap_server_config + ".tls_cert_verify");
        boost::to_lower(tls_cert_verify_lc_str);

        if (tls_cert_verify_lc_str == "never")
            params.tls_cert_verify = LDAPServerParams::TLSCertVerify::NEVER;
        else if (tls_cert_verify_lc_str == "allow")
            params.tls_cert_verify = LDAPServerParams::TLSCertVerify::ALLOW;
        else if (tls_cert_verify_lc_str == "try")
            params.tls_cert_verify = LDAPServerParams::TLSCertVerify::TRY;
        else if (tls_cert_verify_lc_str == "demand")
            params.tls_cert_verify = LDAPServerParams::TLSCertVerify::DEMAND;
        else
            throw Exception("Bad value for 'tls_cert_verify' entry, allowed values are: 'never', 'allow', 'try', 'demand'", ErrorCodes::BAD_ARGUMENTS);
    }

    if (has_ca_cert_dir)
        params.ca_cert_dir = config.getString(ldap_server_config + ".ca_cert_dir");

    if (has_ca_cert_file)
        params.ca_cert_file = config.getString(ldap_server_config + ".ca_cert_file");

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

std::unique_ptr<ExternalAuthenticators> parseExternalAuthenticators(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
{
    auto external_authenticators = std::make_unique<ExternalAuthenticators>();
    parseAndAddLDAPServers(*external_authenticators, config, log);
    return external_authenticators;
}

}
