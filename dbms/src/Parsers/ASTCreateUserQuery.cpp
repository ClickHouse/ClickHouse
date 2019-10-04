#include <Parsers/ASTCreateUserQuery.h>


namespace DB
{
String ASTCreateUserQuery::getID(char) const
{
    return "CreateUserQuery";
}


ASTPtr ASTCreateUserQuery::clone() const
{
    return std::make_shared<ASTCreateUserQuery>(*this);
}


void ASTCreateUserQuery::formatImpl(const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") << "CREATE USER " << (if_not_exists ? "IF NOT EXISTS " : "") << (s.hilite ? hilite_none : "")
           << backQuoteIfNeed(user_name);

    formatAuthentication(s);
    formatAllowedHosts(s);
    formatDefaultRoles(s);
    formatSettings(s);
    formatAccountLock(s);
}


void ASTCreateUserQuery::formatAuthentication(const FormatSettings & s) const
{
    if (!authentication)
        return;

    const auto & auth = *authentication;
    s.ostr << (s.hilite ? hilite_keyword : "") << " IDENTIFY WITH ";

    switch (auth.type)
    {
        case Authentication::NO_PASSWORD: s.ostr << "NO_PASSWORD"; break;
        case Authentication::PLAINTEXT_PASSWORD: s.ostr << "PLAINTEXT_PASSWORD"; break;
        case Authentication::SHA256_PASSWORD: s.ostr << "SHA256_PASSWORD"; break;
        case Authentication::SHA256_HASH: s.ostr << "SHA256_HASH"; break;
        default: __builtin_unreachable();
    }
    s.ostr << (s.hilite ? hilite_none : "");

    if (auth.password)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " BY " << (s.hilite ? hilite_none : "");
        auth.password->format(s);
    }
}


void ASTCreateUserQuery::formatAllowedHosts(const FormatSettings & s) const
{
    if (!allowed_hosts)
        return;

    const auto & ah = *allowed_hosts;
    s.ostr << (s.hilite ? hilite_keyword : "") << " HOST " << (s.hilite ? hilite_none : "");

    if (ah.host_names.empty() && ah.host_regexps.empty() && ah.ip_addresses.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << "NONE" << (s.hilite ? hilite_none : "");
    }
    else
    {
        if (!ah.host_names.empty())
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << "NAME " << (s.hilite ? hilite_none : "");
            for (size_t i = 0; i != ah.host_names.size(); ++i)
            {
                s.ostr << (i ? ", " : "");
                ah.host_names[i]->format(s);
            }
        }
        if (!ah.host_regexps.empty())
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << "REGEXP " << (s.hilite ? hilite_none : "");
            for (size_t i = 0; i != ah.host_regexps.size(); ++i)
            {
                s.ostr << (i ? ", " : "");
                ah.host_regexps[i]->format(s);
            }
        }
        if (!ah.ip_addresses.empty())
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << "IP " << (s.hilite ? hilite_none : "");
            for (size_t i = 0; i != ah.ip_addresses.size(); ++i)
            {
                s.ostr << (i ? ", " : "");
                ah.ip_addresses[i]->format(s);
            }
        }
    }
}


void ASTCreateUserQuery::formatDefaultRoles(const FormatSettings & s) const
{
    if (!default_roles)
        return;

    const auto & dr = *default_roles;
    s.ostr << (s.hilite ? hilite_keyword : "") << " DEFAULT ROLE " << (s.hilite ? hilite_none : "");

    if (dr.role_names.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << "NONE" << (s.hilite ? hilite_none : "");
    }
    else
    {
        for (size_t i = 0; i != dr.role_names.size(); ++i)
            s.ostr << (i ? ", " : "") << backQuoteIfNeed(dr.role_names[i]);
    }
}


void ASTCreateUserQuery::formatSettings(const FormatSettings & s) const
{
    if (!settings)
        return;

    const auto & entries = *settings;
    s.ostr << (s.hilite ? hilite_keyword : "") << " SETTINGS " << (s.hilite ? hilite_none : "");

    if (entries.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " NONE" << (s.hilite ? hilite_none : "");
    }
    else
    {
        for (size_t i = 0; i != entries.size(); ++i)
        {
            const auto & entry = entries[i];
            s.ostr << (i ? ", " : " ") << backQuoteIfNeed(entry.name);
            if (entry.value)
            {
                s.ostr << " = ";
                entry.value->format(s);
            }
            if (entry.min)
            {
                s.ostr << (s.hilite ? hilite_keyword : "") << " MIN " << (s.hilite ? hilite_none : "");
                entry.min->format(s);
            }
            if (entry.max)
            {
                s.ostr << (s.hilite ? hilite_keyword : "") << " MAX " << (s.hilite ? hilite_none : "");
                entry.max->format(s);
            }
            if (entry.read_only)
                s.ostr << (s.hilite ? hilite_keyword : "") << " READONLY" << (s.hilite ? hilite_none : "");
        }
    }
}


void ASTCreateUserQuery::formatAccountLock(const FormatSettings & s) const
{
    if (!account_lock)
        return;

    s.ostr << (s.hilite ? hilite_keyword : "")
           << " ACCOUNT " << (account_lock->locked ? "LOCK" : "UNLOCK")
           << (s.hilite ? hilite_none : "");
}
}
