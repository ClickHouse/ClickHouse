#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Common/quoteString.h>
#include <Access/Common/SSLCertificateSubjects.h>
#include <IO/Operators.h>


namespace DB
{

namespace
{
    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }

    void formatAuthenticationData(const std::vector<std::shared_ptr<ASTAuthenticationData>> & authentication_methods, const IAST::FormatSettings & settings)
    {
        // safe because this method is only called if authentication_methods.size > 1
        // if the first type is present, include the `WITH` keyword
        if (authentication_methods[0]->type)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH" << (settings.hilite ? IAST::hilite_none : "");
        }

        for (std::size_t i = 0; i < authentication_methods.size(); i++)
        {
            authentication_methods[i]->format(settings);

            bool is_last = i < authentication_methods.size() - 1;
            if (is_last)
            {
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : ",");
            }
        }
    }

    void formatValidUntil(const IAST & valid_until, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " VALID UNTIL " << (settings.hilite ? IAST::hilite_none : "");
        valid_until.format(settings);
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
    res->children.clear();
    res->authentication_methods.clear();

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

    for (const auto & authentication_method : authentication_methods)
    {
        auto ast_clone = std::static_pointer_cast<ASTAuthenticationData>(authentication_method->clone());
        res->authentication_methods.push_back(ast_clone);
        res->children.push_back(ast_clone);
    }

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

    if (!storage_name.empty())
        format.ostr << (format.hilite ? IAST::hilite_keyword : "")
                    << " IN " << (format.hilite ? IAST::hilite_none : "")
                    << backQuoteIfNeed(storage_name);

    formatOnCluster(format);

    if (new_name)
        formatRenameTo(*new_name, format);

    if (authentication_methods.empty())
    {
        // If identification (auth method) is missing from query, we should serialize it in the form of `NO_PASSWORD` unless it is alter query
        if (!alter)
        {
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED WITH no_password" << (format.hilite ? IAST::hilite_none : "");
        }
    }
    else
    {
        if (add_identified_with)
        {
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " ADD" << (format.hilite ? IAST::hilite_none : "");
        }

        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED" << (format.hilite ? IAST::hilite_none : "");
        formatAuthenticationData(authentication_methods, format);
    }

    if (valid_until)
        formatValidUntil(*valid_until, format);

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

    if (reset_authentication_methods_to_new)
        format.ostr << (format.hilite ? hilite_keyword : "") << " RESET AUTHENTICATION METHODS TO NEW" << (format.hilite ? hilite_none : "");
}

}
