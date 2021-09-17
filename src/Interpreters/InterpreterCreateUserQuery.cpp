#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetRoleQuery.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTUserNameWithHost.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/ContextAccess.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace
{
    void updateUserFromQueryImpl(
        User & user,
        const ASTCreateUserQuery & query,
        const std::shared_ptr<ASTUserNameWithHost> & override_name,
        const std::optional<RolesOrUsersSet> & override_default_roles,
        const std::optional<SettingsProfileElements> & override_settings,
        const std::optional<RolesOrUsersSet> & override_grantees)
    {
        if (override_name)
            user.setName(override_name->toString());
        else if (!query.new_name.empty())
            user.setName(query.new_name);
        else if (query.names->size() == 1)
            user.setName(query.names->front()->toString());

        if (query.authentication)
            user.authentication = *query.authentication;

        if (override_name && !override_name->host_pattern.empty())
        {
            user.allowed_client_hosts = AllowedClientHosts{};
            user.allowed_client_hosts.addLikePattern(override_name->host_pattern);
        }
        else if (query.hosts)
            user.allowed_client_hosts = *query.hosts;

        if (query.remove_hosts)
            user.allowed_client_hosts.remove(*query.remove_hosts);
        if (query.add_hosts)
            user.allowed_client_hosts.add(*query.add_hosts);

        auto set_default_roles = [&](const RolesOrUsersSet & default_roles_)
        {
            if (!query.alter && !default_roles_.all)
                user.granted_roles.grant(default_roles_.getMatchingIDs());

            InterpreterSetRoleQuery::updateUserSetDefaultRoles(user, default_roles_);
        };

        if (override_default_roles)
            set_default_roles(*override_default_roles);
        else if (query.default_roles)
            set_default_roles(*query.default_roles);

        if (override_settings)
            user.settings = *override_settings;
        else if (query.settings)
            user.settings = *query.settings;

        if (override_grantees)
            user.grantees = *override_grantees;
        else if (query.grantees)
            user.grantees = *query.grantees;
    }
}


BlockIO InterpreterCreateUserQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateUserQuery &>();
    auto & access_control = getContext()->getAccessControlManager();
    auto access = getContext()->getAccess();
    access->checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);

    std::optional<RolesOrUsersSet> default_roles_from_query;
    if (query.default_roles)
    {
        default_roles_from_query = RolesOrUsersSet{*query.default_roles, access_control};
        if (!query.alter && !default_roles_from_query->all)
        {
            for (const UUID & role : default_roles_from_query->getMatchingIDs())
                access->checkAdminOption(role);
        }
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

    if (query.alter)
    {
        std::optional<RolesOrUsersSet> grantees_from_query;
        if (query.grantees)
            grantees_from_query = RolesOrUsersSet{*query.grantees, access_control};

        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
            updateUserFromQueryImpl(*updated_user, query, {}, default_roles_from_query, settings_from_query, grantees_from_query);
            return updated_user;
        };

        Strings names = query.names->toStrings();
        if (query.if_exists)
        {
            auto ids = access_control.find<User>(names);
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(access_control.getIDs<User>(names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_users;
        for (const auto & name : *query.names)
        {
            auto new_user = std::make_shared<User>();
            updateUserFromQueryImpl(*new_user, query, name, default_roles_from_query, settings_from_query, RolesOrUsersSet::AllTag{});
            new_users.emplace_back(std::move(new_user));
        }

        std::vector<UUID> ids;
        if (query.if_not_exists)
            ids = access_control.tryInsert(new_users);
        else if (query.or_replace)
            ids = access_control.insertOrReplace(new_users);
        else
            ids = access_control.insert(new_users);

        if (query.grantees)
        {
            RolesOrUsersSet grantees_from_query = RolesOrUsersSet{*query.grantees, access_control};
            access_control.update(ids, [&](const AccessEntityPtr & entity) -> AccessEntityPtr
            {
                auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
                updated_user->grantees = grantees_from_query;
                return updated_user;
            });
        }
    }

    return {};
}


void InterpreterCreateUserQuery::updateUserFromQuery(User & user, const ASTCreateUserQuery & query)
{
    updateUserFromQueryImpl(user, query, {}, {}, {}, {});
}

}
