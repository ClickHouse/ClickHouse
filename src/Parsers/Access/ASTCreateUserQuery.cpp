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


    void formatAuthenticationData(const AuthenticationData & auth_data, const IAST::FormatSettings & settings)
    {
        auto auth_type = auth_data.getType();
        if (auth_type == AuthenticationType::NO_PASSWORD)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NOT IDENTIFIED"
                          << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        String auth_type_name = AuthenticationTypeInfo::get(auth_type).name;
        String prefix; /// "BY" or "SERVER" or "REALM"
        std::optional<String> password; /// either a password or hash
        std::optional<String> salt;
        std::optional<String> parameter;
        const boost::container::flat_set<String> * parameters = nullptr;

        switch (auth_type)
        {
            case AuthenticationType::PLAINTEXT_PASSWORD:
            {
                prefix = "BY";
                password = auth_data.getPassword();
                break;
            }
            case AuthenticationType::SHA256_PASSWORD:
            {
                auth_type_name = "sha256_hash";
                prefix = "BY";
                password = auth_data.getPasswordHashHex();
                if (!auth_data.getSalt().empty())
                    salt = auth_data.getSalt();
                break;
            }
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            {
                auth_type_name = "double_sha1_hash";
                prefix = "BY";
                password = auth_data.getPasswordHashHex();
                break;
            }
            case AuthenticationType::LDAP:
            {
                prefix = "SERVER";
                parameter = auth_data.getLDAPServerName();
                break;
            }
            case AuthenticationType::KERBEROS:
            {
                const auto & realm = auth_data.getKerberosRealm();
                if (!realm.empty())
                {
                    prefix = "REALM";
                    parameter = realm;
                }
                break;
            }

            case AuthenticationType::SSL_CERTIFICATE:
            {
                prefix = "CN";
                parameters = &auth_data.getSSLCertificateCommonNames();
                break;
            }

            case AuthenticationType::NO_PASSWORD: [[fallthrough]];
            case AuthenticationType::MAX:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "AST: Unexpected authentication type {}", toString(auth_type));
        }

        if (password && !settings.show_secrets)
        {
            prefix = "";
            password.reset();
            salt.reset();
            auth_type_name = AuthenticationTypeInfo::get(auth_type).name;
        }

        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED" << (settings.hilite ? IAST::hilite_none : "");

        if (!auth_type_name.empty())
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH " << auth_type_name
                          << (settings.hilite ? IAST::hilite_none : "");
        }

        if (!prefix.empty())
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << prefix << (settings.hilite ? IAST::hilite_none : "");
        }

        if (password)
        {
            settings.ostr << " " << quoteString(*password);
        }

        if (salt)
        {
            settings.ostr << " SALT " << quoteString(*salt);
        }

        if (parameter)
        {
            settings.ostr << " " << quoteString(*parameter);
        }
        else if (parameters)
        {
            settings.ostr << " ";
            bool need_comma = false;
            for (const auto & param : *parameters)
            {
                if (std::exchange(need_comma, true))
                    settings.ostr << ", ";
                settings.ostr << quoteString(param);
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
    auto res = std::make_shared<ASTCreateUserQuery>(*this);

    if (names)
        res->names = std::static_pointer_cast<ASTUserNamesWithHost>(names->clone());

    if (default_roles)
        res->default_roles = std::static_pointer_cast<ASTRolesOrUsersSet>(default_roles->clone());

    if (default_database)
        res->default_database = std::static_pointer_cast<ASTDatabaseOrNone>(default_database->clone());

    if (grantees)
        res->grantees = std::static_pointer_cast<ASTRolesOrUsersSet>(grantees->clone());

    if (settings)
        res->settings = std::static_pointer_cast<ASTSettingsProfileElements>(settings->clone());

    return res;
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
        formatAuthenticationData(*auth_data, format);

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

bool ASTCreateUserQuery::hasSecretParts() const
{
    if (auth_data)
    {
        auto auth_type = auth_data->getType();
        if ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD)
            || (auth_type == AuthenticationType::SHA256_PASSWORD)
            || (auth_type == AuthenticationType::DOUBLE_SHA1_PASSWORD))
            return true;
    }
    return childrenHaveSecretParts();
}

}
