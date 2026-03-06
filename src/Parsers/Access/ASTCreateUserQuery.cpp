#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace
{
    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << " RENAME TO " << quoteString(new_name);
    }

    void formatAuthenticationData(const std::vector<boost::intrusive_ptr<ASTAuthenticationData>> & authentication_methods, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        // safe because this method is only called if authentication_methods.size > 1
        // if the first type is present, include the `WITH` keyword
        if (authentication_methods[0]->type)
        {
            ostr << " WITH";
        }

        for (std::size_t i = 0; i < authentication_methods.size(); i++)
        {
            authentication_methods[i]->format(ostr, settings);

            bool is_last = i < authentication_methods.size() - 1;
            if (is_last)
                ostr << ",";
        }
    }

    void formatValidUntil(const IAST & valid_until, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " VALID UNTIL ";
        valid_until.format(ostr, settings);
    }

    void formatHosts(const char * prefix, const AllowedClientHosts & hosts, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        if (prefix)
            ostr << " " << prefix << " HOST ";
        else
            ostr << " HOST ";

        if (hosts.empty())
        {
            ostr << "NONE";
            return;
        }

        if (hosts.containsAnyHost())
        {
            ostr << "ANY";
            return;
        }

        bool need_comma = false;
        if (hosts.containsLocalHost())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << "LOCAL";
        }

        const auto & addresses = hosts.getAddresses();
        const auto & subnets = hosts.getSubnets();
        if (!addresses.empty() || !subnets.empty())
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << "IP ";
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
            ostr << "NAME ";
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
            ostr << "REGEXP ";
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
            ostr << "LIKE ";
            bool need_comma2 = false;
            for (const auto & like_pattern : like_patterns)
            {
                if (std::exchange(need_comma2, true))
                    ostr << ", ";
                ostr << quoteString(like_pattern);
            }
        }
    }


    void formatRoles(const ASTRolesOrUsersSet & roles, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " ROLE ";
        roles.format(ostr, settings);
    }

    void formatDefaultRoles(const ASTRolesOrUsersSet & default_roles, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " DEFAULT ROLE ";
        default_roles.format(ostr, settings);
    }

    void formatSettings(const ASTSettingsProfileElements & settings, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << " SETTINGS ";
        settings.format(ostr, format);
    }

    void formatAlterSettings(const ASTAlterSettingsProfileElements & alter_settings, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << " ";
        alter_settings.format(ostr, format);
    }

    void formatGrantees(const ASTRolesOrUsersSet & grantees, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " GRANTEES ";
        grantees.format(ostr, settings);
    }

    void formatDefaultDatabase(const ASTDatabaseOrNone & default_database, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " DEFAULT DATABASE ";
        default_database.format(ostr, settings);
    }
}


String ASTCreateUserQuery::getID(char) const
{
    return "CreateUserQuery";
}


ASTPtr ASTCreateUserQuery::clone() const
{
    auto res = make_intrusive<ASTCreateUserQuery>(*this);
    res->children.clear();
    res->authentication_methods.clear();

    if (names)
        res->names = boost::static_pointer_cast<ASTUserNamesWithHost>(names->clone());

    if (roles)
        res->roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (default_roles)
        res->default_roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(default_roles->clone());

    if (default_database)
        res->default_database = boost::static_pointer_cast<ASTDatabaseOrNone>(default_database->clone());

    if (grantees)
        res->grantees = boost::static_pointer_cast<ASTRolesOrUsersSet>(grantees->clone());

    if (settings)
        res->settings = boost::static_pointer_cast<ASTSettingsProfileElements>(settings->clone());

    if (alter_settings)
        res->alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(alter_settings->clone());

    for (const auto & authentication_method : authentication_methods)
    {
        auto ast_clone = boost::static_pointer_cast<ASTAuthenticationData>(authentication_method->clone());
        res->authentication_methods.push_back(ast_clone);
        res->children.push_back(ast_clone);
    }

    return res;
}


void ASTCreateUserQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
        ostr << "ATTACH USER";
    else
        ostr << (alter ? "ALTER USER" : "CREATE USER");

    if (if_exists)
        ostr << " IF EXISTS";
    else if (if_not_exists)
        ostr << " IF NOT EXISTS";
    else if (or_replace)
        ostr << " OR REPLACE";

    ostr << " ";
    names->format(ostr, format);

    if (!storage_name.empty())
        ostr << " IN " << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, format);

    if (new_name)
        formatRenameTo(*new_name, ostr, format);

    if (authentication_methods.empty())
    {
        // If identification (auth method) is missing from query, we should serialize it in the form of `NO_PASSWORD` unless it is alter query
        if (!alter)
            ostr << " IDENTIFIED WITH no_password";
    }
    else
    {
        if (add_identified_with)
            ostr << " ADD";

        ostr << " IDENTIFIED";
        formatAuthenticationData(authentication_methods, ostr, format);
    }

    if (global_valid_until)
        formatValidUntil(*global_valid_until, ostr, format);

    if (hosts)
        formatHosts(nullptr, *hosts, ostr, format);
    if (add_hosts)
        formatHosts("ADD", *add_hosts, ostr, format);
    if (remove_hosts)
        formatHosts("DROP", *remove_hosts, ostr, format);

    if (default_database)
        formatDefaultDatabase(*default_database, ostr, format);

    if (roles)
        formatRoles(*roles, ostr, format);

    if (default_roles)
        formatDefaultRoles(*default_roles, ostr, format);

    if (alter_settings)
        formatAlterSettings(*alter_settings, ostr, format);
    else if (settings)
        formatSettings(*settings, ostr, format);

    if (grantees)
        formatGrantees(*grantees, ostr, format);

    if (reset_authentication_methods_to_new)
        ostr << " RESET AUTHENTICATION METHODS TO NEW";
}

}
