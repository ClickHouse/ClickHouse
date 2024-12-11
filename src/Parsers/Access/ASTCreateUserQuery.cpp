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
    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }

    void formatAuthenticationData(const std::vector<std::shared_ptr<ASTAuthenticationData>> & authentication_methods, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        // safe because this method is only called if authentication_methods.size > 1
        // if the first type is present, include the `WITH` keyword
        if (authentication_methods[0]->type)
        {
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH" << (settings.hilite ? IAST::hilite_none : "");
        }

        for (std::size_t i = 0; i < authentication_methods.size(); i++)
        {
            authentication_methods[i]->format(ostr, settings);

            bool is_last = i < authentication_methods.size() - 1;
            if (is_last)
            {
                ostr << (settings.hilite ? IAST::hilite_keyword : ",");
            }
        }
    }

    void formatValidUntil(const IAST & valid_until, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " VALID UNTIL " << (settings.hilite ? IAST::hilite_none : "");
        valid_until.format(ostr, settings);
    }

    void formatHosts(const char * prefix, const AllowedClientHosts & hosts, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        if (prefix)
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << prefix << " HOST "
                          << (settings.hilite ? IAST::hilite_none : "");
        else
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << " HOST " << (settings.hilite ? IAST::hilite_none : "");

        if (hosts.empty())
        {
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        if (hosts.containsAnyHost())
        {
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ANY" << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        bool need_comma = false;
        if (hosts.containsLocalHost())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "LOCAL" << (settings.hilite ? IAST::hilite_none : "");
        }

        const auto & addresses = hosts.getAddresses();
        const auto & subnets = hosts.getSubnets();
        if (!addresses.empty() || !subnets.empty())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "IP " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & address : addresses)
            {
                if (std::exchange(need_comma2, true))
                    ostr << ", ";
                ostr << quoteString(address.toString());
            }
            for (const auto & subnet : subnets)
            {
                if (std::exchange(need_comma2, true))
                    ostr << ", ";
                ostr << quoteString(subnet.toString());
            }
        }

        const auto & names = hosts.getNames();
        if (!names.empty())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NAME " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & name : names)
            {
                if (std::exchange(need_comma2, true))
                    ostr << ", ";
                ostr << quoteString(name);
            }
        }

        const auto & name_regexps = hosts.getNameRegexps();
        if (!name_regexps.empty())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "REGEXP " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & host_regexp : name_regexps)
            {
                if (std::exchange(need_comma2, true))
                    ostr << ", ";
                ostr << quoteString(host_regexp);
            }
        }

        const auto & like_patterns = hosts.getLikePatterns();
        if (!like_patterns.empty())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "LIKE " << (settings.hilite ? IAST::hilite_none : "");
            bool need_comma2 = false;
            for (const auto & like_pattern : like_patterns)
            {
                if (std::exchange(need_comma2, true))
                    ostr << ", ";
                ostr << quoteString(like_pattern);
            }
        }
    }


    void formatDefaultRoles(const ASTRolesOrUsersSet & default_roles, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " DEFAULT ROLE " << (settings.hilite ? IAST::hilite_none : "");
        default_roles.format(ostr, settings);
    }


    void formatSettings(const ASTSettingsProfileElements & settings, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << (format.hilite ? IAST::hilite_keyword : "") << " SETTINGS " << (format.hilite ? IAST::hilite_none : "");
        settings.format(ostr, format);
    }


    void formatGrantees(const ASTRolesOrUsersSet & grantees, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " GRANTEES " << (settings.hilite ? IAST::hilite_none : "");
        grantees.format(ostr, settings);
    }

    void formatDefaultDatabase(const ASTDatabaseOrNone & default_database, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " DEFAULT DATABASE " << (settings.hilite ? IAST::hilite_none : "");
        default_database.format(ostr, settings);
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


void ASTCreateUserQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        ostr << (format.hilite ? hilite_keyword : "") << "ATTACH USER" << (format.hilite ? hilite_none : "");
    }
    else
    {
        ostr << (format.hilite ? hilite_keyword : "") << (alter ? "ALTER USER" : "CREATE USER")
                    << (format.hilite ? hilite_none : "");
    }

    if (if_exists)
        ostr << (format.hilite ? hilite_keyword : "") << " IF EXISTS" << (format.hilite ? hilite_none : "");
    else if (if_not_exists)
        ostr << (format.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (format.hilite ? hilite_none : "");
    else if (or_replace)
        ostr << (format.hilite ? hilite_keyword : "") << " OR REPLACE" << (format.hilite ? hilite_none : "");

    ostr << " ";
    names->format(ostr, format);

    if (!storage_name.empty())
        ostr << (format.hilite ? IAST::hilite_keyword : "")
                    << " IN " << (format.hilite ? IAST::hilite_none : "")
                    << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, format);

    if (new_name)
        formatRenameTo(*new_name, ostr, format);

    if (authentication_methods.empty())
    {
        // If identification (auth method) is missing from query, we should serialize it in the form of `NO_PASSWORD` unless it is alter query
        if (!alter)
        {
            ostr << (format.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED WITH no_password" << (format.hilite ? IAST::hilite_none : "");
        }
    }
    else
    {
        if (add_identified_with)
        {
            ostr << (format.hilite ? IAST::hilite_keyword : "") << " ADD" << (format.hilite ? IAST::hilite_none : "");
        }

        ostr << (format.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED" << (format.hilite ? IAST::hilite_none : "");
        formatAuthenticationData(authentication_methods, ostr, format);
    }

    if (global_valid_until)
    {
        formatValidUntil(*global_valid_until, ostr, format);
    }

    if (hosts)
        formatHosts(nullptr, *hosts, ostr, format);
    if (add_hosts)
        formatHosts("ADD", *add_hosts, ostr, format);
    if (remove_hosts)
        formatHosts("DROP", *remove_hosts, ostr, format);

    if (default_database)
        formatDefaultDatabase(*default_database, ostr, format);

    if (default_roles)
        formatDefaultRoles(*default_roles, ostr, format);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, ostr, format);

    if (grantees)
        formatGrantees(*grantees, ostr, format);

    if (reset_authentication_methods_to_new)
        ostr << (format.hilite ? hilite_keyword : "") << " RESET AUTHENTICATION METHODS TO NEW" << (format.hilite ? hilite_none : "");
}

}
