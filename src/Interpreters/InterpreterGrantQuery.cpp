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
    template <typename T>
    void updateFromQueryTemplate(
        T & grantee,
        const ASTGrantQuery & query,
        const std::vector<UUID> & roles_to_grant_or_revoke)
    {
        if (!query.access_rights_elements.empty())
        {
            if (query.is_revoke)
                grantee.access.revoke(query.access_rights_elements);
            else
                grantee.access.grant(query.access_rights_elements);
        }

        if (!roles_to_grant_or_revoke.empty())
        {
            if (query.is_revoke)
            {
                if (query.admin_option)
                    grantee.granted_roles.revokeAdminOption(roles_to_grant_or_revoke);
                else
                    grantee.granted_roles.revoke(roles_to_grant_or_revoke);
            }
            else
            {
                if (query.admin_option)
                    grantee.granted_roles.grantWithAdminOption(roles_to_grant_or_revoke);
                else
                    grantee.granted_roles.grant(roles_to_grant_or_revoke);
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


    void changeGrantOption(AccessRightsElements & elements, bool grant_option)
    {
        std::for_each(elements.begin(), elements.end(), [&](AccessRightsElement & element) { element.grant_option = grant_option; });
    }

    void changeIsRevoke(AccessRightsElements & elements, bool is_revoke)
    {
        std::for_each(elements.begin(), elements.end(), [&](AccessRightsElement & element) { element.is_revoke = is_revoke; });
    }


    void checkGrantOption(const AccessControlManager & access_control, const ContextAccess & access, ASTGrantQuery & query, const std::vector<UUID> & grantees)
    {
        auto & elements = query.access_rights_elements;
        if (!elements.sameOptions())
            throw Exception("Elements in the same ASTGrantQuery must use same options", ErrorCodes::LOGICAL_ERROR);
        if (query.is_revoke)
            changeIsRevoke(elements, true);
        elements.removeNonGrantableFlags();
        if (elements.empty())
            return;

        bool grant_option = elements[0].grant_option;
        bool is_revoke = elements[0].is_revoke;

        if (!is_revoke)
        {
            if (grant_option)
            {
                access.checkAccess(elements);
            }
            else
            {
                /// To execute the command GRANT without GRANT OPTION the current user needs to have the access granted with GRANT OPTION.
                changeGrantOption(elements, true);
                SCOPE_EXIT({ changeGrantOption(elements, grant_option); });
                access.checkAccess(elements);
            }
            return;
        }

        /// To execute the command REVOKE the current user needs to have the access granted with GRANT OPTION.
        changeGrantOption(elements, true);
        changeIsRevoke(elements, false);
        SCOPE_EXIT(
        {
            changeGrantOption(elements, grant_option);
            changeIsRevoke(elements, is_revoke);
        });
        if (access.isGranted(elements))
            return;

        /// Special case for the command REVOKE: it's possible that the current user doesn't have the access granted
        /// with GRANT OPTION but it's still ok because the roles or users from whom the access rights will be
        /// revoked don't have the specified access granted either.
        ///
        /// For example, to execute
        /// GRANT ALL ON mydb.* TO role1
        /// REVOKE ALL ON *.* FROM role1
        /// the current user needs to have grants only on the 'mydb' database.
        AccessRights access_in_use;
        for (const auto & id : grantees)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
                access_in_use.makeUnion(role->access);
            else if (auto user = typeid_cast<UserPtr>(entity))
                access_in_use.makeUnion(user->access);
        }

        AccessRights access_to_revoke;
        changeGrantOption(elements, grant_option);
        access_to_revoke.grant(elements);
        elements.clear();
        access_to_revoke.makeIntersection(access_in_use);

        for (auto & element : access_to_revoke.getElements())
        {
            if (!element.is_revoke && (element.grant_option || !grant_option))
                elements.emplace_back(std::move(element));
        }

        changeGrantOption(elements, true);
        access.checkAccess(elements);
    }


    std::vector<UUID> getRoleIDsAndCheckAdminOption(const AccessControlManager & access_control, const ContextAccess & access, const ASTGrantQuery & query, const std::vector<UUID> & grantees)
    {
        auto roles = RolesOrUsersSet{*query.roles, access_control};
        std::vector<UUID> matching_ids;

        if (!query.is_revoke)
        {
            matching_ids = roles.getMatchingIDs();
            access.checkAdminOption(matching_ids);
            return matching_ids;
        }

        if (!roles.all)
        {
            matching_ids = roles.getMatchingIDs();
            if (access.hasAdminOption(matching_ids))
                return matching_ids;
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have the admin option
        /// for some of the specified roles but it's still ok because the roles or users from whom the roles will be
        /// revoked from don't have the specified roles granted either.
        ///
        /// For example, to execute
        /// GRANT role2 TO role1
        /// REVOKE ALL FROM role1
        /// the current user needs to have only 'role2' to be granted with admin option (not all the roles).
        boost::container::flat_set<UUID> roles_in_use;
        for (const auto & id : grantees)
        {
            auto entity = access_control.tryRead(id);
            auto add_to_roles_in_use = [&](const GrantedRoles & granted_roles)
            {
                if (query.admin_option)
                    roles_in_use.insert(granted_roles.roles_with_admin_option.begin(), granted_roles.roles_with_admin_option.end());
                else
                    roles_in_use.insert(granted_roles.roles.begin(), granted_roles.roles.end());
            };
            if (auto role = typeid_cast<RolePtr>(entity))
                add_to_roles_in_use(role->granted_roles);
            else if (auto user = typeid_cast<UserPtr>(entity))
                add_to_roles_in_use(user->granted_roles);
        }
        if (roles.all)
            boost::range::set_difference(roles_in_use, roles.except_ids, std::back_inserter(matching_ids));
        else
            boost::range::remove_erase_if(matching_ids, [&](const UUID & id) { return !roles_in_use.count(id); });
        access.checkAdminOption(matching_ids);
        return matching_ids;
    }
}


BlockIO InterpreterGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTGrantQuery &>();
    query.replaceCurrentUserTagWithName(context.getUserName());

    if (!query.cluster.empty())
    {
        auto required_access = query.access_rights_elements;
        changeGrantOption(required_access, true);
        changeIsRevoke(required_access, false);
        return executeDDLQueryOnCluster(query_ptr, context, std::move(required_access));
    }

    auto & access_control = context.getAccessControlManager();
    query.replaceEmptyDatabaseWithCurrent(context.getCurrentDatabase());

    std::vector<UUID> grantees = RolesOrUsersSet{*query.grantees, access_control, context.getUserID()}.getMatchingIDs(access_control);

    /// Check if the current user has corresponding access rights with grant option.
    if (!query.access_rights_elements.empty())
        checkGrantOption(access_control, *context.getAccess(), query, grantees);

    /// Check if the current user has corresponding roles granted with admin option.
    std::vector<UUID> roles;
    if (query.roles)
        roles = getRoleIDsAndCheckAdminOption(access_control, *context.getAccess(), query, grantees);

    /// Update roles and users listed in `grantees`.
    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto clone = entity->clone();
        updateFromQueryImpl(*clone, query, roles);
        return clone;
    };

    access_control.update(grantees, update_func);

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
