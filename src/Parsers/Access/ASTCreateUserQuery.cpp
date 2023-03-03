#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{
    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }


    void formatAuthenticationData(const AuthenticationData & auth_data, bool show_password, const IAST::FormatSettings & settings)
    {
        auto auth_type = auth_data.getType();
        if (auth_type == AuthenticationType::NO_PASSWORD)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NOT IDENTIFIED"
                          << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        String auth_type_name = AuthenticationTypeInfo::get(auth_type).name;
        String value_prefix;
        std::optional<String> value;
        const boost::container::flat_set<String> * values = nullptr;

        if (show_password ||
            auth_type == AuthenticationType::LDAP ||
            auth_type == AuthenticationType::KERBEROS ||
            auth_type == AuthenticationType::SSL_CERTIFICATE)
        {
            switch (auth_type)
            {
                case AuthenticationType::PLAINTEXT_PASSWORD:
                {
                    value_prefix = "BY";
                    value = auth_data.getPassword();
                    break;
                }
                case AuthenticationType::SHA256_PASSWORD:
                {
                    auth_type_name = "sha256_hash";
                    value_prefix = "BY";
                    value = auth_data.getPasswordHashHex();
                    break;
                }
                case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                {
                    auth_type_name = "double_sha1_hash";
                    value_prefix = "BY";
                    value = auth_data.getPasswordHashHex();
                    break;
                }
                case AuthenticationType::LDAP:
                {
                    value_prefix = "SERVER";
                    value = auth_data.getLDAPServerName();
                    break;
                }
                case AuthenticationType::KERBEROS:
                {
                    const auto & realm = auth_data.getKerberosRealm();
                    if (!realm.empty())
                    {
                        value_prefix = "REALM";
                        value = realm;
                    }
                    break;
                }

                case AuthenticationType::SSL_CERTIFICATE:
                {
                    value_prefix = "CN";
                    values = &auth_data.getSSLCertificateCommonNames();
                    break;
                }

                case AuthenticationType::NO_PASSWORD: [[fallthrough]];
                case AuthenticationType::MAX:
                    throw Exception("AST: Unexpected authentication type " + toString(auth_type), ErrorCodes::LOGICAL_ERROR);
            }
        }

        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED WITH " << auth_type_name
                      << (settings.hilite ? IAST::hilite_none : "");

        if (!value_prefix.empty())
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << value_prefix
                          << (settings.hilite ? IAST::hilite_none : "");
        }

        if (value)
        {
            settings.ostr << " " << quoteString(*value);
        }
        else if (values)
        {
            settings.ostr << " ";
            bool need_comma = false;
            for (const auto & item : *values)
            {
                if (std::exchange(need_comma, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(item);
            }
        }
    }


    void formatHosts(const char * prefix, const AllowedClientHosts & hosts, const IAST::FormatSettings & settings)
    {
        if (prefix)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << prefix << " HOST "
                          << (settings.hilite ? IAST::hilite_none : "");
        else
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " HOST " << (settings.hilite ? IAST::hilite_none : "");

        if (hosts.empty())
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        if (hosts.containsAnyHost())
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ANY" << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        bool need_comma = false;
        if (hosts.containsLocalHost())
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "LOCAL" << (settings.hilite ? IAST::hilite_none : "");
        }

        const auto & addresses = hosts.getAddresses();
        const auto & subnets = hosts.getSubnets();
        if (!addresses.empty() || !subnets.empty())
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "IP " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & address : addresses)
            {
                if (std::exchange(need_comma2, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(address.toString());
            }
            for (const auto & subnet : subnets)
            {
                if (std::exchange(need_comma2, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(subnet.toString());
            }
        }

        const auto & names = hosts.getNames();
        if (!names.empty())
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NAME " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & name : names)
            {
                if (std::exchange(need_comma2, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(name);
            }
        }

        const auto & name_regexps = hosts.getNameRegexps();
        if (!name_regexps.empty())
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "REGEXP " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & host_regexp : name_regexps)
            {
                if (std::exchange(need_comma2, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(host_regexp);
            }
        }

        const auto & like_patterns = hosts.getLikePatterns();
        if (!like_patterns.empty())
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "LIKE " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & like_pattern : like_patterns)
            {
                if (std::exchange(need_comma2, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(like_pattern);
            }
        }
    }


    void formatDefaultRoles(const ASTRolesOrUsersSet & default_roles, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " DEFAULT ROLE " << (settings.hilite ? IAST::hilite_none : "");
        default_roles.format(settings);
    }


    void formatSettings(const ASTSettingsProfileElements & settings, const IAST::FormatSettings & format)
    {
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " SETTINGS " << (format.hilite ? IAST::hilite_none : "");
        settings.format(format);
    }


    void formatGrantees(const ASTRolesOrUsersSet & grantees, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " GRANTEES " << (settings.hilite ? IAST::hilite_none : "");
        grantees.format(settings);
    }

    void formatDefaultDatabase(const ASTDatabaseOrNone & default_database, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " DEFAULT DATABASE " << (settings.hilite ? IAST::hilite_none : "");
        default_database.format(settings);
    }
}


String ASTCreateUserQuery::getID(char) const
{
    return "CreateUserQuery";
}


ASTPtr ASTCreateUserQuery::clone() const
{
    return std::make_shared<ASTCreateUserQuery>(*this);
}


void ASTCreateUserQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << "ATTACH USER" << (format.hilite ? hilite_none : "");
    }
    else
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (alter ? "ALTER USER" : "CREATE USER")
                    << (format.hilite ? hilite_none : "");
    }

    if (if_exists)
        format.ostr << (format.hilite ? hilite_keyword : "") << " IF EXISTS" << (format.hilite ? hilite_none : "");
    else if (if_not_exists)
        format.ostr << (format.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (format.hilite ? hilite_none : "");
    else if (or_replace)
        format.ostr << (format.hilite ? hilite_keyword : "") << " OR REPLACE" << (format.hilite ? hilite_none : "");

    format.ostr << " ";
    names->format(format);

    formatOnCluster(format);

    if (new_name)
        formatRenameTo(*new_name, format);

    if (auth_data)
        formatAuthenticationData(*auth_data, show_password, format);

    if (hosts)
        formatHosts(nullptr, *hosts, format);
    if (add_hosts)
        formatHosts("ADD", *add_hosts, format);
    if (remove_hosts)
        formatHosts("DROP", *remove_hosts, format);

    if (default_database)
        formatDefaultDatabase(*default_database, format);

    if (default_roles)
        formatDefaultRoles(*default_roles, format);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, format);

    if (grantees)
        formatGrantees(*grantees, format);
}
}
