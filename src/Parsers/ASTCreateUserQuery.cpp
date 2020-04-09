#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTGenericRoleSet.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }


    void formatAuthentication(const Authentication & authentication, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED WITH " << (settings.hilite ? IAST::hilite_none : "");
        switch (authentication.getType())
        {
            case Authentication::Type::NO_PASSWORD:
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "no_password" << (settings.hilite ? IAST::hilite_none : "");
                break;
            case Authentication::Type::PLAINTEXT_PASSWORD:
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "plaintext_password BY " << (settings.hilite ? IAST::hilite_none : "")
                              << quoteString(authentication.getPassword());
                break;
            case Authentication::Type::SHA256_PASSWORD:
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "sha256_hash BY " << (settings.hilite ? IAST::hilite_none : "")
                              << quoteString(authentication.getPasswordHashHex());
                break;
            case Authentication::Type::DOUBLE_SHA1_PASSWORD:
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "double_sha1_hash BY " << (settings.hilite ? IAST::hilite_none : "")
                              << quoteString(authentication.getPasswordHashHex());
                break;
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
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NAME REGEXP " << (settings.hilite ? IAST::hilite_none : "");
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


    void formatDefaultRoles(const ASTGenericRoleSet & default_roles, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " DEFAULT ROLE " << (settings.hilite ? IAST::hilite_none : "");
        default_roles.format(settings);
    }


    void formatProfile(const String & profile_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " PROFILE " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(profile_name);
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


void ASTCreateUserQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ATTACH USER" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (alter ? "ALTER USER" : "CREATE USER")
                      << (settings.hilite ? hilite_none : "");
    }

    if (if_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF EXISTS" << (settings.hilite ? hilite_none : "");
    else if (if_not_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " OR REPLACE" << (settings.hilite ? hilite_none : "");

    settings.ostr << " " << backQuoteIfNeed(name);

    if (!new_name.empty())
        formatRenameTo(new_name, settings);

    if (authentication)
        formatAuthentication(*authentication, settings);

    if (hosts)
        formatHosts(nullptr, *hosts, settings);
    if (add_hosts)
        formatHosts("ADD", *add_hosts, settings);
    if (remove_hosts)
        formatHosts("REMOVE", *remove_hosts, settings);

    if (default_roles)
        formatDefaultRoles(*default_roles, settings);

    if (profile)
        formatProfile(*profile, settings);
}
}
