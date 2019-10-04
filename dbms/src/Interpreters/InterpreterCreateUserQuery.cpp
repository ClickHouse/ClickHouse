#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/User2.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SET_NOT_GRANTED_ROLE;
}


BlockIO InterpreterCreateUserQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateUserQuery &>();

    if (query.alter)
    {
        auto do_update = [&](User2 & user) { extractUserOptionsFromQuery(user, query); };
        context.getAccessControlManager().update(query.user_name, User2::TYPE, do_update);
    }
    else
    {
        User2 user;
        extractUserOptionsFromQuery(user, query);
        if (query.if_not_exists)
            context.getAccessControlManager().tryInsert(user);
        else
            context.getAccessControlManager().insert(user);
    }

    return {};
}


void InterpreterCreateUserQuery::extractUserOptionsFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (!query.alter)
        user.name = query.user_name;

    extractAuthenticationFromQuery(user, query);
    extractAllowedHostsFromQuery(user, query);
    extractDefaultRolesFromQuery(user, query);
    extractSettingsFromQuery(user, query);
    extractAccountLockFromQuery(user, query);
}


void InterpreterCreateUserQuery::extractAuthenticationFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (!query.authentication)
        return;

    using Authentication = ASTCreateUserQuery::Authentication;
    const Authentication & auth = *query.authentication;

    switch (auth.type)
    {
        case Authentication::NO_PASSWORD:
            user.password.clear();
            break;
        case Authentication::PLAINTEXT_PASSWORD:
            user.password.setPassword(EncryptedPassword::PLAINTEXT, auth.password->as<const ASTLiteral &>().value.safeGet<String>());
            break;
        case Authentication::SHA256_PASSWORD:
            user.password.setPassword(EncryptedPassword::SHA256, auth.password->as<const ASTLiteral &>().value.safeGet<String>());
            break;
        case Authentication::SHA256_HASH:
            user.password.setHashHex(EncryptedPassword::SHA256, auth.password->as<const ASTLiteral &>().value.safeGet<String>());
            break;
    }
}


void InterpreterCreateUserQuery::extractAllowedHostsFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (!query.allowed_hosts)
        return;

    const auto & ah = *query.allowed_hosts;

    user.allowed_hosts.clear();
    for (const auto & name_ast : ah.host_names)
        user.allowed_hosts.addHostName(name_ast->as<ASTLiteral &>().value.safeGet<String>());

    for (const auto & regexp_ast : ah.host_regexps)
        user.allowed_hosts.addHostRegexp(regexp_ast->as<ASTLiteral &>().value.safeGet<String>());

    for (const auto & ip_address_ast : ah.ip_addresses)
    {
        String ip_address = ip_address_ast->as<ASTLiteral &>().value.safeGet<String>();
        size_t slash = ip_address.find('/');
        if (slash == String::npos)
        {
            user.allowed_hosts.addIPAddress(Poco::Net::IPAddress(ip_address));
        }
        else
        {
            String prefix(ip_address, 0, slash);
            String mask(ip_address, slash + 1, ip_address.length() - slash - 1);
            if (std::all_of(mask.begin(), mask.end(), isNumericASCII))
                user.allowed_hosts.addIPSubnet(Poco::Net::IPAddress(prefix), parseFromString<UInt8>(mask));
            else
                user.allowed_hosts.addIPSubnet(Poco::Net::IPAddress(prefix), Poco::Net::IPAddress(mask));
        }
    }
}


void InterpreterCreateUserQuery::extractDefaultRolesFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (!query.default_roles)
        return;

    const auto & dr = *query.default_roles;

    user.default_roles.clear();
    if (dr.all_granted)
    {
        for (const auto & granted_roles : user.granted_roles_by_admin_option)
            user.default_roles.insert(granted_roles.begin(), granted_roles.end());
        return;
    }

    for (const String & role_name : dr.role_names)
    {
        UUID role_id = context.getAccessControlManager().getID<Role>(role_name);
        if (query.alter)
        {
            bool role_is_granted = false;
            for (const auto & granted_roles : user.granted_roles_by_admin_option)
            {
                if (granted_roles.count(role_id))
                {
                    role_is_granted = true;
                    break;
                }
            }
            if (!role_is_granted)
                throw Exception(
                    "User " + user.name + ": Cannot set role " + role_name + " to be default because it's not granted",
                    ErrorCodes::SET_NOT_GRANTED_ROLE);
        }
        else
        {
            user.granted_roles_by_admin_option[false].emplace(role_id);
        }
        user.default_roles.emplace(role_id);
    }
}


void InterpreterCreateUserQuery::extractSettingsFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (!query.settings)
        return;

    const auto & entries = *query.settings;
    user.settings.clear();
    user.settings_constraints.clear();

    for (const auto & entry : entries)
    {
        const String & name = entry.name;
        if (entry.value)
            user.settings.emplace_back(SettingChange{name, entry.value->as<const ASTLiteral &>().value});

        if (entry.min)
            user.settings_constraints.setMinValue(name, entry.min->as<const ASTLiteral &>().value);

        if (entry.max)
            user.settings_constraints.setMaxValue(name, entry.max->as<const ASTLiteral &>().value);

        if (entry.read_only)
            user.settings_constraints.setReadOnly(name, true);
    }
}


void InterpreterCreateUserQuery::extractAccountLockFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (!query.account_lock)
        return;

    user.account_locked = query.account_lock->locked;
}
}
