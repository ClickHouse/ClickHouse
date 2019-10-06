#include <Parsers/ASTCreateUserQuery.h>
#include <Common/FieldVisitors.h>
#include <IO/WriteBufferFromString.h>
#include <map>


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

    switch (auth.getType())
    {
        case Authentication::NO_PASSWORD: s.ostr << "NO_PASSWORD"; break;
        case Authentication::PLAINTEXT_PASSWORD: s.ostr << "PLAINTEXT_PASSWORD"; break;
        case Authentication::SHA256_PASSWORD: s.ostr << "SHA256_HASH"; break;
        default: __builtin_unreachable();
    }
    s.ostr << (s.hilite ? hilite_none : "");

    if (auth.getType() != Authentication::NO_PASSWORD)
        s.ostr << (s.hilite ? hilite_keyword : "") << " BY " << (s.hilite ? hilite_none : "") << auth.getPasswordHash();
}


void ASTCreateUserQuery::formatAllowedHosts(const FormatSettings & s) const
{
    if (!allowed_hosts)
        return;

    const auto & ah = *allowed_hosts;
    const auto & ip_addresses = ah.getIPAddresses();
    const auto & ip_subnets = ah.getIPSubnets();
    const auto & host_names = ah.getHostNames();
    const auto & host_regexps = ah.getHostRegexps();

    s.ostr << (s.hilite ? hilite_keyword : "") << " HOST" << (s.hilite ? hilite_none : "");

    if (ip_addresses.empty() && ip_subnets.empty() && host_names.empty() && host_regexps.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " NONE" << (s.hilite ? hilite_none : "");
        return;
    }

    if (std::count(ip_subnets.begin(), ip_subnets.end(), AllowedHosts::IPSubnet::ALL_ADDRESSES)
        || std::count(host_regexps.begin(), host_regexps.end(), ".*")
        || std::count(host_regexps.begin(), host_regexps.end(), "$"))
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " ANY" << (s.hilite ? hilite_none : "");
        return;
    }

    if (!ip_addresses.empty() || !ip_subnets.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " IP " << (s.hilite ? hilite_none : "");
        for (size_t i = 0; i != ip_addresses.size(); ++i)
            s.ostr << (i ? ", " : "") << quoteString(ip_addresses[i].toString());
        for (size_t i = 0; i != ip_subnets.size(); ++i)
            s.ostr << ((i + ip_addresses.size()) ? ", " : "") << quoteString(ip_subnets[i].toString());
    }

    if (!host_names.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " NAME " << (s.hilite ? hilite_none : "");
        for (size_t i = 0; i != host_names.size(); ++i)
            s.ostr << (i ? ", " : "") << quoteString(host_names[i]);
    }

    if (!host_regexps.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " REGEXP " << (s.hilite ? hilite_none : "");
        for (size_t i = 0; i != host_regexps.size(); ++i)
            s.ostr << (i ? ", " : "") << quoteString(host_regexps[i]);
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
    if (!settings && !settings_constraints)
        return;

    struct Entry
    {
        Field value;
        Field min;
        Field max;
        bool read_only = false;
    };
    std::map<String, Entry> entries;

    if (settings)
    {
        for (const auto & setting : *settings)
            entries[setting.name].value = setting.value;
    }

    if (settings_constraints)
    {
        for (const auto & constraint : settings_constraints->getInfo())
        {
            auto & out = entries[constraint.name.toString()];
            out.min = constraint.min;
            out.max = constraint.max;
            out.read_only = constraint.read_only;
        }
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << " SETTINGS " << (s.hilite ? hilite_none : "");
    if (entries.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " NONE" << (s.hilite ? hilite_none : "");
    }
    else
    {
        size_t index = 0;
        for (const auto & [name, entry] : entries)
        {
            s.ostr << (index++ ? ", " : " ") << backQuoteIfNeed(name);
            if (!entry.value.isNull())
                s.ostr << " = " << applyVisitor(FieldVisitorToString(), entry.value);

            if (!entry.min.isNull())
                s.ostr << (s.hilite ? hilite_keyword : "") << " MIN " << (s.hilite ? hilite_none : "")
                       << applyVisitor(FieldVisitorToString(), entry.min);

            if (!entry.max.isNull())
                s.ostr << (s.hilite ? hilite_keyword : "") << " MAX " << (s.hilite ? hilite_none : "")
                       << applyVisitor(FieldVisitorToString(), entry.max);

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
           << " ACCOUNT " << (account_lock->account_locked ? "LOCK" : "UNLOCK")
           << (s.hilite ? hilite_none : "");
}
}
