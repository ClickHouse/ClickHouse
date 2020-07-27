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
namespace
{
    template <typename T>
    void updateFromQueryImpl(T & grantee, const ASTGrantQuery & query, const std::vector<UUID> & roles_from_query, const String & current_database, bool partial_revokes)
    {
        using Kind = ASTGrantQuery::Kind;
        if (!query.access_rights_elements.empty())
        {
            if (query.kind == Kind::GRANT)
            {
                grantee.access.grant(query.access_rights_elements, current_database);
                if (query.grant_option)
                    grantee.access_with_grant_option.grant(query.access_rights_elements, current_database);
            }
            else if (partial_revokes)
            {
                grantee.access_with_grant_option.partialRevoke(query.access_rights_elements, current_database);
                if (!query.grant_option)
                    grantee.access.partialRevoke(query.access_rights_elements, current_database);
            }
            else
            {
                grantee.access_with_grant_option.revoke(query.access_rights_elements, current_database);
                if (!query.grant_option)
                    grantee.access.revoke(query.access_rights_elements, current_database);
            }
        }

        if (!roles_from_query.empty())
        {
            if (query.kind == Kind::GRANT)
            {
                boost::range::copy(roles_from_query, std::inserter(grantee.granted_roles, grantee.granted_roles.end()));
                if (query.admin_option)
                    boost::range::copy(roles_from_query, std::inserter(grantee.granted_roles_with_admin_option, grantee.granted_roles_with_admin_option.end()));
            }
            else
            {
                for (const UUID & role_from_query : roles_from_query)
                {
                    grantee.granted_roles_with_admin_option.erase(role_from_query);
                    if (!query.admin_option)
                        grantee.granted_roles.erase(role_from_query);
                    if constexpr (std::is_same_v<T, User>)
                        grantee.default_roles.ids.erase(role_from_query);
                }
            }
        }
    }
}


BlockIO InterpreterGrantQuery::execute()
{
    const auto & query = query_ptr->as<const ASTGrantQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.getAccessRights()->checkGrantOption(query.access_rights_elements);

    std::vector<UUID> roles_from_query;
    if (query.roles)
    {
        roles_from_query = GenericRoleSet{*query.roles, access_control}.getMatchingRoles(access_control);
        for (const UUID & role_from_query : roles_from_query)
            context.getAccessRights()->checkAdminOption(role_from_query);
    }

    std::vector<UUID> to_roles = GenericRoleSet{*query.to_roles, access_control, context.getUserID()}.getMatchingUsersAndRoles(access_control);
    String current_database = context.getCurrentDatabase();
    bool partial_revokes = context.getSettingsRef().partial_revokes;

    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto clone = entity->clone();
        if (auto user = typeid_cast<std::shared_ptr<User>>(clone))
        {
            updateFromQueryImpl(*user, query, roles_from_query, current_database, partial_revokes);
            return user;
        }
        else if (auto role = typeid_cast<std::shared_ptr<Role>>(clone))
        {
            updateFromQueryImpl(*role, query, roles_from_query, current_database, partial_revokes);
            return role;
        }
        else
            return entity;
    };

    access_control.update(to_roles, update_func);

    return {};
}


void InterpreterGrantQuery::updateUserFromQuery(User & user, const ASTGrantQuery & query)
{
    std::vector<UUID> roles_from_query;
    if (query.roles)
        roles_from_query = GenericRoleSet{*query.roles}.getMatchingIDs();
    updateFromQueryImpl(user, query, roles_from_query, {}, true);
}


void InterpreterGrantQuery::updateRoleFromQuery(Role & role, const ASTGrantQuery & query)
{
    std::vector<UUID> roles_from_query;
    if (query.roles)
        roles_from_query = GenericRoleSet{*query.roles}.getMatchingIDs();
    updateFromQueryImpl(role, query, roles_from_query, {}, true);
}

}
