#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}
namespace
{
    void updateUserFromQueryImpl(
        User & user,
        const ASTCreateUserQuery & query,
        const std::shared_ptr<ASTUserNameWithHost> & override_name,
        const std::optional<RolesOrUsersSet> & override_default_roles,
        const std::optional<SettingsProfileElements> & override_settings,
        const std::optional<RolesOrUsersSet> & override_grantees, bool allow_no_password, bool allow_plaintext_password)
    {
        if (override_name)
            user.setName(override_name->toString());
        else if (!query.new_name.empty())
            user.setName(query.new_name);
        else if (query.names->size() == 1)
            user.setName(query.names->front()->toString());
        if (query.auth_data)
        {
            user.auth_data = *query.auth_data;
            //User and  query IDENTIFIED WITH AUTHTYPE PLAINTEXT and NO_PASSWORD should not be allowed if allow_plaintext_and_no_password is unset.
            if ((query.auth_data->getType() == AuthenticationType::PLAINTEXT_PASSWORD  && !allow_plaintext_password) || (query.auth_data->getType() == AuthenticationType::NO_PASSWORD &&  !allow_no_password))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "User is not allowed to ALTER/CREATE USERS with type "+ toString(query.auth_data->getType())+". Please configure User with authtype"
                            + "to SHA256_PASSWORD,DOUBLE_SHA1_PASSWORD OR enable setting allow_plaintext_and_no_password in server configuration to configure user with " + toString(query.auth_data->getType()) +" Auth_type."
                            + "It is not recommended to use " + toString(query.auth_data->getType()) + ".");
        }
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

        if (query.default_database)
            user.default_database = query.default_database->database_name;

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
    auto & access_control = getContext()->getAccessControl();
    auto access = getContext()->getAccess();
    access->checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);
    bool allow_plaintext_password = access_control.isPlaintextPasswordAllowed();
    bool allow_no_password = access_control.isNoPasswordAllowed();

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
            updateUserFromQueryImpl(*updated_user, query, {}, default_roles_from_query, settings_from_query, grantees_from_query, allow_no_password, allow_plaintext_password);
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
            updateUserFromQueryImpl(*new_user, query, name, default_roles_from_query, settings_from_query, RolesOrUsersSet::AllTag{}, allow_no_password, allow_plaintext_password);
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


void InterpreterCreateUserQuery::updateUserFromQuery(User & user, const ASTCreateUserQuery & query, bool allow_no_password, bool allow_plaintext_password)
{
    updateUserFromQueryImpl(user, query, {}, {}, {}, {}, allow_no_password, allow_plaintext_password);
}

}
