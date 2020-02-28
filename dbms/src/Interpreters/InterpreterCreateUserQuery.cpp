#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetRoleQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/GenericRoleSet.h>
#include <Access/AccessRightsContext.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace ErrorCodes
{
}


BlockIO InterpreterCreateUserQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateUserQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);

    GenericRoleSet * default_roles_from_query = nullptr;
    GenericRoleSet temp_role_set;
    if (query.default_roles)
    {
        default_roles_from_query = &temp_role_set;
        *default_roles_from_query = GenericRoleSet{*query.default_roles, access_control};
        if (!query.alter && !default_roles_from_query->all)
        {
            for (const UUID & role : default_roles_from_query->getMatchingIDs())
                context.getAccessRights()->checkAdminOption(role);
        }
    }

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
            updateUserFromQuery(*updated_user, query, default_roles_from_query);
            return updated_user;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<User>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<User>(query.name), update_func);
    }
    else
    {
        auto new_user = std::make_shared<User>();
        updateUserFromQuery(*new_user, query, default_roles_from_query);

        if (query.if_not_exists)
            access_control.tryInsert(new_user);
        else if (query.or_replace)
            access_control.insertOrReplace(new_user);
        else
            access_control.insert(new_user);
    }

    return {};
}


void InterpreterCreateUserQuery::updateUserFromQuery(User & user, const ASTCreateUserQuery & query, const GenericRoleSet * default_roles_from_query)
{
    if (query.alter)
    {
        if (!query.new_name.empty())
            user.setName(query.new_name);
    }
    else
        user.setName(query.name);

    if (query.authentication)
        user.authentication = *query.authentication;

    if (query.hosts)
        user.allowed_client_hosts = *query.hosts;
    if (query.remove_hosts)
        user.allowed_client_hosts.remove(*query.remove_hosts);
    if (query.add_hosts)
        user.allowed_client_hosts.add(*query.add_hosts);

    if (default_roles_from_query)
    {
        if (!query.alter && !default_roles_from_query->all)
            boost::range::copy(default_roles_from_query->getMatchingIDs(), std::inserter(user.granted_roles, user.granted_roles.end()));

        InterpreterSetRoleQuery::updateUserSetDefaultRoles(user, *default_roles_from_query);
    }

    if (query.profile)
        user.profile = *query.profile;
}

}
