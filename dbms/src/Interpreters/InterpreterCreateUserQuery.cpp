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

    std::vector<UUID> default_role_ids;
    if (query.default_roles)
    {
        for (const auto & role_name : query.default_roles->role_names)
            default_role_ids.emplace_back(context.getAccessControlManager().getID<Role>(role_name));
    }

    if (query.alter)
    {
        auto do_update = [&](User2 & user) { extractUserOptionsFromQuery(user, query, default_role_ids); };
        context.getAccessControlManager().update(query.user_name, User2::TYPE, do_update);
    }
    else
    {
        User2 user;
        extractUserOptionsFromQuery(user, query, default_role_ids);
        if (query.if_not_exists)
            context.getAccessControlManager().tryInsert(user);
        else
            context.getAccessControlManager().insert(user);
    }

    return {};
}


void InterpreterCreateUserQuery::extractUserOptionsFromQuery(User2 & user, const ASTCreateUserQuery & query, const std::vector<UUID> & default_role_ids) const
{
    if (!query.alter)
        user.name = query.user_name;

    extractAuthenticationFromQuery(user, query);
    extractAllowedHostsFromQuery(user, query);
    extractDefaultRolesFromQuery(user, query, default_role_ids);
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


void InterpreterCreateUserQuery::extractDefaultRolesFromQuery(User2 & user, const ASTCreateUserQuery & query, const std::vector<UUID> & default_role_ids) const
{
    if (!query.default_roles)
        return;

    user.default_roles.clear();
    if (query.default_roles->all_granted)
    {
        for (const auto & granted_roles : user.granted_roles)
            user.default_roles.insert(granted_roles.begin(), granted_roles.end());
        return;
    }

    for (size_t i = 0; i != default_role_ids.size(); ++i)
    {
        const UUID & role_id = default_role_ids[i];
        if (query.alter)
        {
            bool role_is_granted = false;
            for (const auto & granted_roles : user.granted_roles)
            {
                if (granted_roles.count(role_id))
                {
                    role_is_granted = true;
                    break;
                }
            }
            if (!role_is_granted)
                throw Exception(
                    "User " + user.name + ": Cannot set role " + query.default_roles->role_names[i]
                        + " to be default because it's not granted",
                    ErrorCodes::SET_NOT_GRANTED_ROLE);
        }
        else
        {
            user.granted_roles[false].emplace(role_id);
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
