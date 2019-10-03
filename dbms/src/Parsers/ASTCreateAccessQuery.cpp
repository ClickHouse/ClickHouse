#include <Parsers/ASTCreateAccessQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Access/User2.h>


namespace DB
{
String ASTAuthentication::getID(char) const
{
    return "Authentication";
}


ASTPtr ASTAuthentication::clone() const
{
    return std::make_shared<ASTAuthentication>(*this);
}


void ASTAuthentication::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " IDENTIFY WITH ";
    switch (type)
    {
        case NO_PASSWORD: settings.ostr << "NO_PASSWORD"; break;
        case PLAINTEXT_PASSWORD: settings.ostr << "PLAINTEXT_PASSWORD"; break;
        case SHA256_PASSWORD: settings.ostr << "SHA256_PASSWORD"; break;
        case SHA256_HASH: settings.ostr << "SHA256_HASH"; break;
        default: __builtin_unreachable();
    }
    settings.ostr << (settings.hilite ? hilite_none : "");

    if (password)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " BY " << (settings.hilite ? hilite_none : "");
        password->format(settings);
    }
}


String ASTAllowedHosts::getID(char) const
{
    return "AllowedHosts";
}


ASTPtr ASTAllowedHosts::clone() const
{
    return std::make_shared<ASTAllowedHosts>(*this);
}


ASTAllowedHosts::ASTAllowedHosts(const String & /*host_pattern*/)
{

}


void ASTAllowedHosts::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " HOST " << (settings.hilite ? hilite_none : "");
    if (host_names.empty() && host_regexps.empty() && ip_addresses.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "NONE" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        if (!host_names.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "NAME " << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i != host_names.size(); ++i)
            {
                settings.ostr << (i ? ", " : "");
                host_names[i]->format(settings);
            }
        }
        if (!host_regexps.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "REGEXP " << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i != host_regexps.size(); ++i)
            {
                settings.ostr << (i ? ", " : "");
                host_regexps[i]->format(settings);
            }
        }
        if (!ip_addresses.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "IP " << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i != ip_addresses.size(); ++i)
            {
                settings.ostr << (i ? ", " : "");
                ip_addresses[i]->format(settings);
            }
        }
    }
}


String ASTDefaultRoles::getID(char) const
{
    return "DefaultRoles";
}


ASTPtr ASTDefaultRoles::clone() const
{
    return std::make_shared<ASTDefaultRoles>(*this);
}


void ASTDefaultRoles::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " DEFAULT ROLE " << (settings.hilite ? hilite_none : "");
    if (role_names.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "NONE" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        for (size_t i = 0; i != role_names.size(); ++i)
            settings.ostr << (i ? ", " : "") << role_names[i];
    }
}


String ASTCreateRoleQuery::getID(char) const
{
    return "CreateRoleQuery";
}


ASTPtr ASTCreateRoleQuery::clone() const
{
    return std::make_shared<ASTCreateRoleQuery>(*this);
}


void ASTCreateRoleQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "CREATE ROLE "
                  << (if_not_exists ? "IF NOT EXISTS " : "")
                  << (settings.hilite ? hilite_none : "");
    for (size_t i = 0; i != role_names.size(); ++i)
        settings.ostr << (i ? ", " : "") << role_names[i];
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
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "CREATE USER "
                  << (if_not_exists ? "IF NOT EXISTS " : "")
                  << (settings.hilite ? hilite_none : "")
                  << user_name;

    if (authentication)
        authentication->format(settings);

    if (allowed_hosts)
        allowed_hosts->format(settings);

    if (default_roles)
        default_roles->format(settings);

    if (settings_with_constraints)
        settings_with_constraints->format(settings);

    if (account_lock)
        account_lock->format(settings);
}
}
