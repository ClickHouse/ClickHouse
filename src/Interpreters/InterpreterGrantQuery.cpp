#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/QueryLog.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    template <typename T>
    void updateFromQueryTemplate(
        T & grantee,
        const ASTGrantQuery & query,
        const std::vector<UUID> & roles_to_grant_or_revoke)
    {
        if (!query.is_revoke)
        {
            if (query.replace_access)
                grantee.access = {};
            if (query.replace_granted_roles)
                grantee.granted_roles = {};
        }


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

    void checkGranteeIsAllowed(const ContextAccess & access, const UUID & grantee_id, const IAccessEntity & grantee)
    {
        auto current_user = access.getUser();
        if (current_user && !current_user->grantees.match(grantee_id))
            throw Exception(grantee.outputTypeAndName() + " is not allowed as grantee", ErrorCodes::ACCESS_DENIED);
    }

    void checkGranteesAreAllowed(const AccessControlManager & access_control, const ContextAccess & access, const std::vector<UUID> & grantee_ids)
    {
        auto current_user = access.getUser();
        if (!current_user || (current_user->grantees == RolesOrUsersSet::AllTag{}))
            return;

        for (const auto & id : grantee_ids)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
                checkGranteeIsAllowed(access, id, *role);
            else if (auto user = typeid_cast<UserPtr>(entity))
                checkGranteeIsAllowed(access, id, *user);
        }
    }

    void checkGrantOption(
        const AccessControlManager & access_control,
        const ContextAccess & access,
        const ASTGrantQuery & query,
        const std::vector<UUID> & grantees_from_query,
        bool & need_check_grantees_are_allowed)
    {
        const auto & elements = query.access_rights_elements;
        need_check_grantees_are_allowed = true;
        if (elements.empty())
        {
            /// No access rights to grant or revoke.
            need_check_grantees_are_allowed = false;
            return;
        }

        if (!query.is_revoke)
        {
            /// To execute the command GRANT the current user needs to have the access granted with GRANT OPTION.
            access.checkGrantOption(elements);
            return;
        }

        if (access.hasGrantOption(elements))
        {
            /// Simple case: the current user has the grant option for all the access rights specified for REVOKE.
            return;
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have
        /// the access granted with GRANT OPTION but it's still ok because the roles or users
        /// from whom the access rights will be revoked don't have the specified access granted either.
        ///
        /// For example, to execute
        /// GRANT ALL ON mydb.* TO role1
        /// REVOKE ALL ON *.* FROM role1
        /// the current user needs to have grants only on the 'mydb' database.
        AccessRights all_granted_access;
        for (const auto & id : grantees_from_query)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
            {
                checkGranteeIsAllowed(access, id, *role);
                all_granted_access.makeUnion(role->access);
            }
            else if (auto user = typeid_cast<UserPtr>(entity))
            {
                checkGranteeIsAllowed(access, id, *user);
                all_granted_access.makeUnion(user->access);
            }
        }
        need_check_grantees_are_allowed = false; /// already checked

        AccessRights required_access;
        if (elements[0].is_partial_revoke)
        {
            AccessRightsElements non_revoke_elements = elements;
            std::for_each(non_revoke_elements.begin(), non_revoke_elements.end(), [&](AccessRightsElement & element) { element.is_partial_revoke = false; });
            required_access.grant(non_revoke_elements);
        }
        else
        {
            required_access.grant(elements);
        }
        required_access.makeIntersection(all_granted_access);

        for (auto & required_access_element : required_access.getElements())
        {
            if (!required_access_element.is_partial_revoke && (required_access_element.grant_option || !elements[0].grant_option))
                access.checkGrantOption(required_access_element);
        }
    }

    std::vector<UUID> getRoleIDsAndCheckAdminOption(
        const AccessControlManager & access_control,
        const ContextAccess & access,
        const ASTGrantQuery & query,
        const RolesOrUsersSet & roles_from_query,
        const std::vector<UUID> & grantees_from_query,
        bool & need_check_grantees_are_allowed)
    {
        need_check_grantees_are_allowed = true;
        if (roles_from_query.empty())
        {
            /// No roles to grant or revoke.
            need_check_grantees_are_allowed = false;
            return {};
        }

        std::vector<UUID> matching_ids;
        if (!query.is_revoke)
        {
            /// To execute the command GRANT the current user needs to have the roles granted with ADMIN OPTION.
            matching_ids = roles_from_query.getMatchingIDs(access_control);
            access.checkAdminOption(matching_ids);
            return matching_ids;
        }

        if (!roles_from_query.all)
        {
            matching_ids = roles_from_query.getMatchingIDs();
            if (access.hasAdminOption(matching_ids))
            {
                /// Simple case: the current user has the admin option for all the roles specified for REVOKE.
                return matching_ids;
            }
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have the admin option
        /// for some of the specified roles but it's still ok because the roles or users from whom the roles will be
        /// revoked from don't have the specified roles granted either.
        ///
        /// For example, to execute
        /// GRANT role2 TO role1
        /// REVOKE ALL FROM role1
        /// the current user needs to have only 'role2' to be granted with admin option (not all the roles).
        GrantedRoles all_granted_roles;
        for (const auto & id : grantees_from_query)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
            {
                checkGranteeIsAllowed(access, id, *role);
                all_granted_roles.makeUnion(role->granted_roles);
            }
            else if (auto user = typeid_cast<UserPtr>(entity))
            {
                checkGranteeIsAllowed(access, id, *user);
                all_granted_roles.makeUnion(user->granted_roles);
            }
        }
        need_check_grantees_are_allowed = false; /// already checked

        const auto & all_granted_roles_set = query.admin_option ? all_granted_roles.getGrantedWithAdminOption() : all_granted_roles.getGranted();
        if (roles_from_query.all)
            boost::range::set_difference(all_granted_roles_set, roles_from_query.except_ids, std::back_inserter(matching_ids));
        else
            boost::range::remove_erase_if(matching_ids, [&](const UUID & id) { return !all_granted_roles_set.count(id); });
        access.checkAdminOption(matching_ids);
        return matching_ids;
    }

    void checkGrantOptionAndGrantees(
        const AccessControlManager & access_control,
        const ContextAccess & access,
        const ASTGrantQuery & query,
        const std::vector<UUID> & grantees_from_query)
    {
        bool need_check_grantees_are_allowed = true;
        checkGrantOption(access_control, access, query, grantees_from_query, need_check_grantees_are_allowed);
        if (need_check_grantees_are_allowed)
            checkGranteesAreAllowed(access_control, access, grantees_from_query);
    }

    std::vector<UUID> getRoleIDsAndCheckAdminOptionAndGrantees(
        const AccessControlManager & access_control,
        const ContextAccess & access,
        const ASTGrantQuery & query,
        const RolesOrUsersSet & roles_from_query,
        const std::vector<UUID> & grantees_from_query)
    {
        bool need_check_grantees_are_allowed = true;
        auto role_ids = getRoleIDsAndCheckAdminOption(
            access_control, access, query, roles_from_query, grantees_from_query, need_check_grantees_are_allowed);
        if (need_check_grantees_are_allowed)
            checkGranteesAreAllowed(access_control, access, grantees_from_query);
        return role_ids;
    }
}


BlockIO InterpreterGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTGrantQuery &>();

    query.replaceCurrentUserTag(getContext()->getUserName());
    query.access_rights_elements.eraseNonGrantable();

    if (!query.access_rights_elements.sameOptions())
        throw Exception("Elements of an ASTGrantQuery are expected to have the same options", ErrorCodes::LOGICAL_ERROR);
    if (!query.access_rights_elements.empty() && query.access_rights_elements[0].is_partial_revoke && !query.is_revoke)
        throw Exception("A partial revoke should be revoked, not granted", ErrorCodes::LOGICAL_ERROR);

    auto & access_control = getContext()->getAccessControlManager();
    std::optional<RolesOrUsersSet> roles_set;
    if (query.roles)
        roles_set = RolesOrUsersSet{*query.roles, access_control};

    std::vector<UUID> grantees = RolesOrUsersSet{*query.grantees, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);

    /// Check if the current user has corresponding roles granted with admin option.
    std::vector<UUID> roles;
    if (roles_set)
        roles = getRoleIDsAndCheckAdminOptionAndGrantees(access_control, *getContext()->getAccess(), query, *roles_set, grantees);

    if (!query.cluster.empty())
    {
        /// To execute the command GRANT the current user needs to have the access granted with GRANT OPTION.
        auto required_access = query.access_rights_elements;
        std::for_each(required_access.begin(), required_access.end(), [&](AccessRightsElement & element) { element.grant_option = true; });
        checkGranteesAreAllowed(access_control, *getContext()->getAccess(), grantees);
        return executeDDLQueryOnCluster(query_ptr, getContext(), std::move(required_access));
    }

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    /// Check if the current user has corresponding access rights with grant option.
    if (!query.access_rights_elements.empty())
        checkGrantOptionAndGrantees(access_control, *getContext()->getAccess(), query, grantees);

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

void InterpreterGrantQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    auto & query = query_ptr->as<ASTGrantQuery &>();
    if (query.is_revoke)
        elem.query_kind = "Revoke";
    else
        elem.query_kind = "Grant";
}

}
