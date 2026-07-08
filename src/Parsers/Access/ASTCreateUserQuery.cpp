#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Common/SipHash.h>
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

    if (!authentication_methods.empty())
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


namespace
{
    void updateHashWithHosts(SipHash & hash_state, const std::optional<AllowedClientHosts> & hosts)
    {
        /// The formatter emits a `HOST` clause only when the optional holds a value (and `HOST NONE`
        /// for an empty value), so fold the presence flag and the individual, formatter-emitted
        /// host descriptors (`address` / `subnet` are folded as their `toString`, mirroring the
        /// formatter).
        hash_state.update(hosts.has_value());
        if (!hosts)
            return;

        hash_state.update(hosts->containsAnyHost());
        hash_state.update(hosts->containsLocalHost());

        const auto & addresses = hosts->getAddresses();
        hash_state.update(addresses.size());
        for (const auto & address : addresses)
            hash_state.update(address.toString());

        const auto & subnets = hosts->getSubnets();
        hash_state.update(subnets.size());
        for (const auto & subnet : subnets)
            hash_state.update(subnet.toString());

        auto update_strings = [&](const std::vector<String> & values)
        {
            hash_state.update(values.size());
            for (const auto & value : values)
                hash_state.update(value);
        };
        update_strings(hosts->getNames());
        update_strings(hosts->getNameRegexps());
        update_strings(hosts->getLikePatterns());
    }
}


void ASTCreateUserQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);

    hash_state.update(alter);
    hash_state.update(attach);
    hash_state.update(if_exists);
    hash_state.update(if_not_exists);
    hash_state.update(or_replace);
    hash_state.update(reset_authentication_methods_to_new);
    hash_state.update(add_identified_with);
    /// `replace_authentication_methods` is intentionally not folded: it is not emitted by the
    /// formatter, so folding it would break the format -> parse round-trip.

    hash_state.update(storage_name);
    hash_state.update(cluster);

    hash_state.update(new_name.has_value());
    if (new_name)
        hash_state.update(*new_name);

    /// `authentication_methods` and `global_valid_until` are kept in `children` and are already
    /// hashed by the base `IAST::updateTreeHashImpl`. Everything below is kept outside `children`.
    /// Fold a presence flag before each optional member so that, for example, `CREATE USER u ROLE r`
    /// and `CREATE USER u GRANTEES r` (both a single `ASTRolesOrUsersSet`) cannot collide.
    hash_state.update(static_cast<bool>(names));
    if (names)
        names->updateTreeHash(hash_state, ignore_aliases);

    updateHashWithHosts(hash_state, hosts);
    updateHashWithHosts(hash_state, add_hosts);
    updateHashWithHosts(hash_state, remove_hosts);

    hash_state.update(static_cast<bool>(default_database));
    if (default_database)
        default_database->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(roles));
    if (roles)
        roles->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(default_roles));
    if (default_roles)
        default_roles->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(settings));
    if (settings)
        settings->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(alter_settings));
    if (alter_settings)
        alter_settings->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(grantees));
    if (grantees)
        grantees->updateTreeHash(hash_state, ignore_aliases);
}

}
