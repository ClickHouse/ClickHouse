#include <Interpreters/InterpreterGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessRightsContext.h>
#include <Access/GenericRoleSet.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
BlockIO InterpreterGrantQuery::execute()
{
    const auto & query = query_ptr->as<const ASTGrantQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.getAccessRights()->checkGrantOption(query.access_rights_elements);

    using Kind = ASTGrantQuery::Kind;
    std::vector<UUID> roles;
    if (query.roles)
    {
        roles = GenericRoleSet{*query.roles, access_control}.getMatchingRoles(access_control);
        for (const UUID & role : roles)
            context.getAccessRights()->checkAdminOption(role);
    }

    std::vector<UUID> to_roles = GenericRoleSet{*query.to_roles, access_control, context.getUserID()}.getMatchingUsersAndRoles(access_control);
    String current_database = context.getCurrentDatabase();
    using Kind = ASTGrantQuery::Kind;

    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto clone = entity->clone();
        AccessRights * access = nullptr;
        AccessRights * access_with_grant_option = nullptr;
        boost::container::flat_set<UUID> * granted_roles = nullptr;
        boost::container::flat_set<UUID> * granted_roles_with_admin_option = nullptr;
        GenericRoleSet * default_roles = nullptr;
        if (auto user = typeid_cast<std::shared_ptr<User>>(clone))
        {
            access = &user->access;
            access_with_grant_option = &user->access_with_grant_option;
            granted_roles = &user->granted_roles;
            granted_roles_with_admin_option = &user->granted_roles_with_admin_option;
            default_roles = &user->default_roles;
        }
        else if (auto role = typeid_cast<std::shared_ptr<Role>>(clone))
        {
            access = &role->access;
            access_with_grant_option = &role->access_with_grant_option;
            granted_roles = &role->granted_roles;
            granted_roles_with_admin_option = &role->granted_roles_with_admin_option;
        }
        else
            return entity;

        if (!query.access_rights_elements.empty())
        {
            if (query.kind == Kind::GRANT)
            {
                access->grant(query.access_rights_elements, current_database);
                if (query.grant_option)
                    access_with_grant_option->grant(query.access_rights_elements, current_database);
            }
            else if (context.getSettingsRef().partial_revokes)
            {
                access_with_grant_option->partialRevoke(query.access_rights_elements, current_database);
                if (!query.grant_option)
                    access->partialRevoke(query.access_rights_elements, current_database);
            }
            else
            {
                access_with_grant_option->revoke(query.access_rights_elements, current_database);
                if (!query.grant_option)
                    access->revoke(query.access_rights_elements, current_database);
            }
        }

        if (!roles.empty())
        {
            if (query.kind == Kind::GRANT)
            {
                boost::range::copy(roles, std::inserter(*granted_roles, granted_roles->end()));
                if (query.admin_option)
                    boost::range::copy(roles, std::inserter(*granted_roles_with_admin_option, granted_roles_with_admin_option->end()));
            }
            else
            {
                for (const UUID & role : roles)
                {
                    granted_roles_with_admin_option->erase(role);
                    if (!query.admin_option)
                    {
                        granted_roles->erase(role);
                        if (default_roles)
                            default_roles->ids.erase(role);
                    }
                }
            }
        }
        return clone;
    };

    access_control.update(to_roles, update_func);

    return {};
}

}
