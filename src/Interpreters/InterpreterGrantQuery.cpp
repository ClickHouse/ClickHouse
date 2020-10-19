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

    template <typename T>
    void updateFromQueryTemplate(
        T & grantee,
        const ASTGrantQuery & query,
        const std::vector<UUID> & roles_to_grant_or_revoke)
    {
        if (!query.access_rights_elements.empty())
        {
            if (query.kind == Kind::GRANT)
            {
                if (query.grant_option)
                    grantee.access.grantWithGrantOption(query.access_rights_elements);
                else
                    grantee.access.grant(query.access_rights_elements);
            }
            else
            {
                if (query.grant_option)
                    grantee.access.revokeGrantOption(query.access_rights_elements);
                else
                    grantee.access.revoke(query.access_rights_elements);
            }
        }

        if (!roles_to_grant_or_revoke.empty())
        {
            if (query.kind == Kind::GRANT)
            {
                if (query.admin_option)
                    grantee.granted_roles.grantWithAdminOption(roles_to_grant_or_revoke);
                else
                    grantee.granted_roles.grant(roles_to_grant_or_revoke);
            }
            else
            {
                if (query.admin_option)
                    grantee.granted_roles.revokeAdminOption(roles_to_grant_or_revoke);
                else
                    grantee.granted_roles.revoke(roles_to_grant_or_revoke);
            }
        }
    }

    void updateFromQueryImpl(
        IAccessEntity & grantee,
        const ASTGrantQuery & query,
        const std::vector<UUID> & roles_to_grant_or_revoke)
    {
        if (auto * user = typeid_cast<User *>(&grantee))
            updateFromQueryTemplate(*user, query, roles_to_grant_or_revoke);
        else if (auto * role = typeid_cast<Role *>(&grantee))
            updateFromQueryTemplate(*role, query, roles_to_grant_or_revoke);
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

    RolesOrUsersSet roles_set;
    if (query.roles)
        roles_set = RolesOrUsersSet{*query.roles, access_control};

    std::vector<UUID> to_roles = RolesOrUsersSet{*query.to_roles, access_control, context.getUserID()}.getMatchingIDs(access_control);

    /// Check if the current user has corresponding access rights with grant option.
    if (!query.access_rights_elements.empty())
    {
        query.access_rights_elements.removeNonGrantableFlags();

        /// Special case for REVOKE: it's possible that the current user doesn't have the grant option for all
        /// the specified access rights and that's ok because the roles or users which the access rights
        /// will be revoked from don't have the specified access rights either.
        ///
        /// For example, to execute
        /// GRANT ALL ON mydb.* TO role1
        /// REVOKE ALL ON *.* FROM role1
        /// the current user needs to have access rights only for the 'mydb' database.
        if ((query.kind == Kind::REVOKE) && !access->hasGrantOption(query.access_rights_elements))
        {
            AccessRights max_access;
            for (const auto & id : to_roles)
            {
                auto entity = access_control.tryRead(id);
                if (auto role = typeid_cast<RolePtr>(entity))
                    max_access.makeUnion(role->access);
                else if (auto user = typeid_cast<UserPtr>(entity))
                    max_access.makeUnion(user->access);
            }
            AccessRights access_to_revoke;
            if (query.grant_option)
                access_to_revoke.grantWithGrantOption(query.access_rights_elements);
            else
                access_to_revoke.grant(query.access_rights_elements);
            access_to_revoke.makeIntersection(max_access);
            AccessRightsElements filtered_access_to_revoke;
            for (auto & element : access_to_revoke.getElements())
            {
                if ((element.kind == Kind::GRANT) && (element.grant_option || !query.grant_option))
                    filtered_access_to_revoke.emplace_back(std::move(element));
            }
            query.access_rights_elements = std::move(filtered_access_to_revoke);
        }

        access->checkGrantOption(query.access_rights_elements);
    }

    /// Check if the current user has corresponding roles granted with admin option.
    std::vector<UUID> roles_to_grant_or_revoke;
    if (!roles_set.empty())
    {
        bool all = roles_set.all;
        if (!all)
            roles_to_grant_or_revoke = roles_set.getMatchingIDs();

        /// Special case for REVOKE: it's possible that the current user doesn't have the admin option for all
        /// the specified roles and that's ok because the roles or users which the roles will be revoked from
        /// don't have the specified roles granted either.
        ///
        /// For example, to execute
        /// GRANT role2 TO role1
        /// REVOKE ALL FROM role1
        /// the current user needs to have only 'role2' to be granted with admin option (not all the roles).
        if ((query.kind == Kind::REVOKE) && (roles_set.all || !access->hasAdminOption(roles_to_grant_or_revoke)))
        {
            auto & roles_to_revoke = roles_to_grant_or_revoke;
            boost::container::flat_set<UUID> max_roles;
            for (const auto & id : to_roles)
            {
                auto entity = access_control.tryRead(id);
                auto add_to_max_roles = [&](const GrantedRoles & granted_roles)
                {
                    if (query.admin_option)
                        max_roles.insert(granted_roles.roles_with_admin_option.begin(), granted_roles.roles_with_admin_option.end());
                    else
                        max_roles.insert(granted_roles.roles.begin(), granted_roles.roles.end());
                };
                if (auto role = typeid_cast<RolePtr>(entity))
                    add_to_max_roles(role->granted_roles);
                else if (auto user = typeid_cast<UserPtr>(entity))
                    add_to_max_roles(user->granted_roles);
            }
            if (roles_set.all)
                boost::range::set_difference(max_roles, roles_set.except_ids, std::back_inserter(roles_to_revoke));
            else
                boost::range::remove_erase_if(roles_to_revoke, [&](const UUID & id) { return !max_roles.count(id); });
        }

        access->checkAdminOption(roles_to_grant_or_revoke);
    }

    /// Update roles and users listed in `to_roles`.
    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto clone = entity->clone();
        updateFromQueryImpl(*clone, query, roles_to_grant_or_revoke);
        return clone;
    };

    access_control.update(to_roles, update_func);

    return {};
}


void InterpreterGrantQuery::updateUserFromQuery(User & user, const ASTGrantQuery & query)
{
    std::vector<UUID> roles_to_grant_or_revoke;
    if (query.roles)
        roles_to_grant_or_revoke = RolesOrUsersSet{*query.roles}.getMatchingIDs();
    updateFromQueryImpl(user, query, roles_to_grant_or_revoke);
}


void InterpreterGrantQuery::updateRoleFromQuery(Role & role, const ASTGrantQuery & query)
{
    std::vector<UUID> roles_to_grant_or_revoke;
    if (query.roles)
        roles_to_grant_or_revoke = RolesOrUsersSet{*query.roles}.getMatchingIDs();
    updateFromQueryImpl(role, query, roles_to_grant_or_revoke);
}

}
