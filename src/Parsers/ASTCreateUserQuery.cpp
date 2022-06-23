#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTUserNameWithHost.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/ASTSettingsProfileElement.h>
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


    void formatAuthentication(const Authentication & authentication, bool show_password, const IAST::FormatSettings & settings)
    {
        auto authentication_type = authentication.getType();
        if (authentication_type == Authentication::NO_PASSWORD)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NOT IDENTIFIED"
                          << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        String authentication_type_name = Authentication::TypeInfo::get(authentication_type).name;
        String by_keyword = "BY";
        std::optional<String> by_value;

        if (
            show_password ||
            authentication_type == Authentication::LDAP ||
            authentication_type == Authentication::KERBEROS
        )
        {
            switch (authentication_type)
            {
                case Authentication::PLAINTEXT_PASSWORD:
                {
                    by_value = authentication.getPassword();
                    break;
                }
                case Authentication::SHA256_PASSWORD:
                {
                    authentication_type_name = "sha256_hash";
                    by_value = authentication.getPasswordHashHex();
                    break;
                }
                case Authentication::DOUBLE_SHA1_PASSWORD:
                {
                    authentication_type_name = "double_sha1_hash";
                    by_value = authentication.getPasswordHashHex();
                    break;
                }
                case Authentication::LDAP:
                {
                    by_keyword = "SERVER";
                    by_value = authentication.getLDAPServerName();
                    break;
                }
                case Authentication::KERBEROS:
                {
                    by_keyword = "REALM";
                    const auto & realm = authentication.getKerberosRealm();
                    if (!realm.empty())
                        by_value = realm;
                    break;
                }

                case Authentication::NO_PASSWORD: [[fallthrough]];
                case Authentication::MAX_TYPE:
                    throw Exception("AST: Unexpected authentication type " + toString(authentication_type), ErrorCodes::LOGICAL_ERROR);
            }
        }

        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED WITH " << authentication_type_name
                      << (settings.hilite ? IAST::hilite_none : "");

        if (by_value)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << by_keyword << " "
                          << (settings.hilite ? IAST::hilite_none : "") << quoteString(*by_value);
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

    if (authentication)
        formatAuthentication(*authentication, show_password, format);

    if (hosts)
        formatHosts(nullptr, *hosts, format);
    if (add_hosts)
        formatHosts("ADD", *add_hosts, format);
    if (remove_hosts)
        formatHosts("DROP", *remove_hosts, format);

    if (default_roles)
        formatDefaultRoles(*default_roles, format);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, format);

    if (grantees)
        formatGrantees(*grantees, format);
}
}
