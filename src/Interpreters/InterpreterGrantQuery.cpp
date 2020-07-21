#include <Interpreters/InterpreterGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>


namespace DB
{
namespace
{
    using Kind = ASTGrantQuery::Kind;

    void doGrantAccess(
        AccessRights & current_access,
        const AccessRightsElements & access_to_grant,
        bool with_grant_option)
    {
        if (with_grant_option)
            current_access.grantWithGrantOption(access_to_grant);
        else
            current_access.grant(access_to_grant);
    }


    AccessRightsElements getFilteredAccessRightsElementsToRevoke(
        const AccessRights & current_access, const AccessRightsElements & access_to_revoke, bool grant_option)
    {
        AccessRights intersection;
        if (grant_option)
            intersection.grantWithGrantOption(access_to_revoke);
        else
            intersection.grant(access_to_revoke);
        intersection.makeIntersection(current_access);

        AccessRightsElements res;
        for (auto & element : intersection.getElements())
        {
            if ((element.kind == Kind::GRANT) && (element.grant_option || !grant_option))
                res.emplace_back(std::move(element));
        }

        return res;
    }

    void doRevokeAccess(
        AccessRights & current_access,
        const AccessRightsElements & access_to_revoke,
        bool grant_option,
        const std::shared_ptr<const ContextAccess> & context)
    {
        if (context && !context->hasGrantOption(access_to_revoke))
            context->checkGrantOption(getFilteredAccessRightsElementsToRevoke(current_access, access_to_revoke, grant_option));

        if (grant_option)
            current_access.revokeGrantOption(access_to_revoke);
        else
            current_access.revoke(access_to_revoke);
    }


    void doGrantRoles(GrantedRoles & granted_roles,
                      const RolesOrUsersSet & roles_to_grant,
                      bool with_admin_option)
    {
        auto ids = roles_to_grant.getMatchingIDs();

        if (with_admin_option)
            granted_roles.grantWithAdminOption(ids);
        else
            granted_roles.grant(ids);
    }


    std::vector<UUID>
    getFilteredListOfRolesToRevoke(const GrantedRoles & granted_roles, const RolesOrUsersSet & roles_to_revoke, bool admin_option)
    {
        std::vector<UUID> ids;
        if (roles_to_revoke.all)
        {
            boost::range::set_difference(
                admin_option ? granted_roles.roles_with_admin_option : granted_roles.roles,
                roles_to_revoke.except_ids,
                std::back_inserter(ids));
        }
        else
        {
            boost::range::set_intersection(
                        admin_option ? granted_roles.roles_with_admin_option : granted_roles.roles,
                        roles_to_revoke.getMatchingIDs(),
                        std::back_inserter(ids));
        }
        return ids;
    }

    void doRevokeRoles(GrantedRoles & granted_roles,
                       RolesOrUsersSet * default_roles,
                       const RolesOrUsersSet & roles_to_revoke,
                       bool admin_option,
                       const std::unordered_map<UUID, String> & names_of_roles,
                       const std::shared_ptr<const ContextAccess> & context)
    {
        auto ids = getFilteredListOfRolesToRevoke(granted_roles, roles_to_revoke, admin_option);

        if (context)
            context->checkAdminOption(ids, names_of_roles);

        if (admin_option)
            granted_roles.revokeAdminOption(ids);
        else
        {
            granted_roles.revoke(ids);
            if (default_roles)
            {
                for (const UUID & id : ids)
                    default_roles->ids.erase(id);
                for (const UUID & id : ids)
                    default_roles->except_ids.erase(id);
            }
        }
    }


    template <typename T>
    void collectRoleNamesTemplate(
        std::unordered_map<UUID, String> & names_of_roles,
        const T & grantee,
        const ASTGrantQuery & query,
        const RolesOrUsersSet & roles_from_query,
        const AccessControlManager & access_control)
    {
        for (const auto & id : getFilteredListOfRolesToRevoke(grantee.granted_roles, roles_from_query, query.admin_option))
        {
            auto name = access_control.tryReadName(id);
            if (name)
                names_of_roles.emplace(id, std::move(*name));
        }
    }

    void collectRoleNames(
        std::unordered_map<UUID, String> & names_of_roles,
        const IAccessEntity & grantee,
        const ASTGrantQuery & query,
        const RolesOrUsersSet & roles_from_query,
        const AccessControlManager & access_control)
    {
        if (const auto * user = typeid_cast<const User *>(&grantee))
            collectRoleNamesTemplate(names_of_roles, *user, query, roles_from_query, access_control);
        else if (const auto * role = typeid_cast<const Role *>(&grantee))
            collectRoleNamesTemplate(names_of_roles, *role, query, roles_from_query, access_control);
    }


    template <typename T>
    void updateFromQueryTemplate(
        T & grantee,
        const ASTGrantQuery & query,
        const RolesOrUsersSet & roles_from_query,
        const std::unordered_map<UUID, String> & names_of_roles,
        const std::shared_ptr<const ContextAccess> & context)
    {
        if (!query.access_rights_elements.empty())
        {
            if (query.kind == Kind::GRANT)
                doGrantAccess(grantee.access, query.access_rights_elements, query.grant_option);
            else
                doRevokeAccess(grantee.access, query.access_rights_elements, query.grant_option, context);
        }

        if (!roles_from_query.empty())
        {
            if (query.kind == Kind::GRANT)
                doGrantRoles(grantee.granted_roles, roles_from_query, query.admin_option);
            else
            {
                RolesOrUsersSet * grantee_default_roles = nullptr;
                if constexpr (std::is_same_v<T, User>)
                    grantee_default_roles = &grantee.default_roles;
                doRevokeRoles(grantee.granted_roles, grantee_default_roles, roles_from_query, query.admin_option, names_of_roles, context);
            }
        }
    }

    void updateFromQueryImpl(
        IAccessEntity & grantee,
        const ASTGrantQuery & query,
        const RolesOrUsersSet & roles_from_query,
        const std::unordered_map<UUID, String> & names_or_roles,
        const std::shared_ptr<const ContextAccess> & context)
    {
        if (auto * user = typeid_cast<User *>(&grantee))
            updateFromQueryTemplate(*user, query, roles_from_query, names_or_roles, context);
        else if (auto * role = typeid_cast<Role *>(&grantee))
            updateFromQueryTemplate(*role, query, roles_from_query, names_or_roles, context);
    }
}


BlockIO InterpreterGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTGrantQuery &>();
    query.replaceCurrentUserTagWithName(context.getUserName());

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, query.access_rights_elements, true);

    auto access = context.getAccess();
    auto & access_control = context.getAccessControlManager();
    query.replaceEmptyDatabaseWithCurrent(context.getCurrentDatabase());

    RolesOrUsersSet roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles, access_control};

    std::vector<UUID> to_roles = RolesOrUsersSet{*query.to_roles, access_control, context.getUserID()}.getMatchingIDs(access_control);

    std::unordered_map<UUID, String> names_of_roles;
    if (!roles_from_query.empty() && (query.kind == Kind::REVOKE))
    {
        for (const auto & id : to_roles)
        {
            auto entity = access_control.tryRead(id);
            if (entity)
                collectRoleNames(names_of_roles, *entity, query, roles_from_query, access_control);
        }
    }

    if (query.kind == Kind::GRANT) /// For Kind::REVOKE the grant/admin option is checked inside updateFromQueryImpl().
    {
        if (!query.access_rights_elements.empty())
            access->checkGrantOption(query.access_rights_elements);

        if (!roles_from_query.empty())
            access->checkAdminOption(roles_from_query.getMatchingIDs());
    }

    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto clone = entity->clone();
        updateFromQueryImpl(*clone, query, roles_from_query, names_of_roles, access);
        return clone;
    };

    access_control.update(to_roles, update_func);

    return {};
}


void InterpreterGrantQuery::updateUserFromQuery(User & user, const ASTGrantQuery & query)
{
    RolesOrUsersSet roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles};
    updateFromQueryImpl(user, query, roles_from_query, {}, nullptr);
}


void InterpreterGrantQuery::updateRoleFromQuery(Role & role, const ASTGrantQuery & query)
{
    RolesOrUsersSet roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles};
    updateFromQueryImpl(role, query, roles_from_query, {}, nullptr);
}

}
