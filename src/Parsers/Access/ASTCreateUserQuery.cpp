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
    void formatRenameTo(const String & new_name, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" RENAME TO ");
        out.ostr << quoteString(new_name);
    }


    void formatAuthenticationData(const AuthenticationData & auth_data, const IAST::FormattingBuffer & out)
    {
        auto auth_type = auth_data.getType();
        if (auth_type == AuthenticationType::NO_PASSWORD)
        {
            out.writeKeyword(" NOT IDENTIFIED");
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

        if (password && !out.shouldShowSecrets())
        {
            prefix = "";
            password.reset();
            salt.reset();
            auth_type_name = AuthenticationTypeInfo::get(auth_type).name;
        }

        out.writeKeyword(" IDENTIFIED");

        if (!auth_type_name.empty())
        {
            out.writeKeyword(" WITH ");
            out.writeKeyword(auth_type_name);
        }

        if (!prefix.empty())
        {
            out.ostr << " ";
            out.writeKeyword(prefix);
        }

        if (password)
        {
            out.ostr << " " << quoteString(*password);
        }

        if (salt)
        {
            out.ostr << " SALT " << quoteString(*salt);
        }

        if (parameter)
        {
            out.ostr << " " << quoteString(*parameter);
        }
        else if (parameters)
        {
            out.ostr << " ";
            bool need_comma = false;
            for (const auto & param : *parameters)
            {
                if (std::exchange(need_comma, true))
                    out.ostr << ", ";
                out.ostr << quoteString(param);
            }
        }
    }


    void formatHosts(const char * prefix, const AllowedClientHosts & hosts, const IAST::FormattingBuffer & out)
    {
        if (prefix)
        {
            out.ostr << " ";
            out.writeKeyword(prefix);
        }
        out.writeKeyword(" HOST ");

        if (hosts.empty())
        {
            out.writeKeyword("NONE");
            return;
        }

        if (hosts.containsAnyHost())
        {
            out.writeKeyword("ANY");
            return;
        }

        bool need_comma = false;
        if (hosts.containsLocalHost())
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.writeKeyword("LOCAL");
        }

        const auto & addresses = hosts.getAddresses();
        const auto & subnets = hosts.getSubnets();
        if (!addresses.empty() || !subnets.empty())
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.writeKeyword("IP ");
            bool need_comma2 = false;
            for (const auto & address : addresses)
            {
                if (std::exchange(need_comma2, true))
                    out.ostr << ", ";
                out.ostr << quoteString(address.toString());
            }
            for (const auto & subnet : subnets)
            {
                if (std::exchange(need_comma2, true))
                    out.ostr << ", ";
                out.ostr << quoteString(subnet.toString());
            }
        }

        const auto & names = hosts.getNames();
        if (!names.empty())
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.writeKeyword("NAME ");
            bool need_comma2 = false;
            for (const auto & name : names)
            {
                if (std::exchange(need_comma2, true))
                    out.ostr << ", ";
                out.ostr << quoteString(name);
            }
        }

        const auto & name_regexps = hosts.getNameRegexps();
        if (!name_regexps.empty())
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.writeKeyword("REGEXP ");
            bool need_comma2 = false;
            for (const auto & host_regexp : name_regexps)
            {
                if (std::exchange(need_comma2, true))
                    out.ostr << ", ";
                out.ostr << quoteString(host_regexp);
            }
        }

        const auto & like_patterns = hosts.getLikePatterns();
        if (!like_patterns.empty())
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.writeKeyword("LIKE ");
            bool need_comma2 = false;
            for (const auto & like_pattern : like_patterns)
            {
                if (std::exchange(need_comma2, true))
                    out.ostr << ", ";
                out.ostr << quoteString(like_pattern);
            }
        }
    }


    void formatDefaultRoles(const ASTRolesOrUsersSet & default_roles, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" DEFAULT ROLE ");
        default_roles.format(out);
    }


    void formatSettings(const ASTSettingsProfileElements & settings, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" SETTINGS ");
        settings.format(out);
    }


    void formatGrantees(const ASTRolesOrUsersSet & grantees, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" GRANTEES ");
        grantees.format(out);
    }

    void formatDefaultDatabase(const ASTDatabaseOrNone & default_database, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" DEFAULT DATABASE ");
        default_database.format(out);
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


void ASTCreateUserQuery::formatImpl(const FormattingBuffer & out) const
{
    if (attach)
    {
        out.writeKeyword("ATTACH USER");
    }
    else
    {
        out.writeKeyword(alter ? "ALTER USER" : "CREATE USER");
    }

    if (if_exists)
        out.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        out.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        out.writeKeyword(" OR REPLACE");

    out.ostr << " ";
    names->format(out);

    formatOnCluster(out);

    if (new_name)
        formatRenameTo(*new_name, out.copy());

    if (auth_data)
        formatAuthenticationData(*auth_data, out.copy());

    if (hosts)
        formatHosts(nullptr, *hosts, out.copy());
    if (add_hosts)
        formatHosts("ADD", *add_hosts, out.copy());
    if (remove_hosts)
        formatHosts("DROP", *remove_hosts, out.copy());

    if (default_database)
        formatDefaultDatabase(*default_database, out.copy());

    if (default_roles)
        formatDefaultRoles(*default_roles, out.copy());

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, out.copy());

    if (grantees)
        formatGrantees(*grantees, out.copy());
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
