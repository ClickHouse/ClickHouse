#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/User2.h>
#include <Common/Exception.h>


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
    if (query.authentication)
        user.authentication = *query.authentication;
}


void InterpreterCreateUserQuery::extractAllowedHostsFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (query.allowed_hosts)
        user.allowed_hosts = *query.allowed_hosts;
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
    if (query.settings)
        user.settings = *query.settings;

    if (query.settings_constraints)
        user.settings_constraints = *query.settings_constraints;
}


void InterpreterCreateUserQuery::extractAccountLockFromQuery(User2 & user, const ASTCreateUserQuery & query) const
{
    if (query.account_lock)
        user.account_locked = query.account_lock->account_locked;
}
}
