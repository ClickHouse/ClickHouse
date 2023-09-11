#include <Interpreters/Access/InterpreterCreateUserQuery.h>

#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/User.h>
#include <Common/logger_useful.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
}
namespace
{
    void updateUserFromQueryImpl(
        User & user,
        const ASTCreateUserQuery & query,
        const std::optional<AuthenticationData> auth_data,
        const std::shared_ptr<ASTUserNameWithHost> & override_name,
        const std::optional<RolesOrUsersSet> & override_default_roles,
        const std::optional<SettingsProfileElements> & override_settings,
        const std::optional<RolesOrUsersSet> & override_grantees,
        bool allow_implicit_no_password,
        bool allow_no_password,
        bool allow_plaintext_password)
    {
        if (override_name)
            user.setName(override_name->toString());
        else if (query.new_name)
            user.setName(*query.new_name);
        else if (query.names->size() == 1)
            user.setName(query.names->front()->toString());

        if (!query.attach && !query.alter && !auth_data && !allow_implicit_no_password)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Authentication type NO_PASSWORD must "
                            "be explicitly specified, check the setting allow_implicit_no_password "
                            "in the server configuration");

        if (auth_data)
            user.auth_data = *auth_data;

        if (auth_data || !query.alter)
        {
            auto auth_type = user.auth_data.getType();
            if (((auth_type == AuthenticationType::NO_PASSWORD) && !allow_no_password) ||
                ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD)  && !allow_plaintext_password))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Authentication type {} is not allowed, check the setting allow_{} in the server configuration",
                                toString(auth_type),
                                AuthenticationTypeInfo::get(auth_type).name);
            }
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
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query_ptr->as<const ASTCreateUserQuery &>();

    auto & access_control = getContext()->getAccessControl();
    auto access = getContext()->getAccess();
    access->checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);
    bool implicit_no_password_allowed = access_control.isImplicitNoPasswordAllowed();
    bool no_password_allowed = access_control.isNoPasswordAllowed();
    bool plaintext_password_allowed = access_control.isPlaintextPasswordAllowed();

    std::optional<AuthenticationData> auth_data;
    if (query.auth_data)
        auth_data = AuthenticationData::fromAST(*query.auth_data, getContext(), !query.attach);

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

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
    {
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

        if (!query.attach)
            getContext()->checkSettingsConstraints(*settings_from_query, SettingSource::USER);
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;

    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    Strings names = query.names->toStrings();
    if (query.alter)
    {
        std::optional<RolesOrUsersSet> grantees_from_query;
        if (query.grantees)
            grantees_from_query = RolesOrUsersSet{*query.grantees, access_control};

        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
            updateUserFromQueryImpl(*updated_user, query, auth_data, {}, default_roles_from_query, settings_from_query, grantees_from_query, implicit_no_password_allowed, no_password_allowed, plaintext_password_allowed);
            return updated_user;
        };

        if (query.if_exists)
        {
            auto ids = storage->find<User>(names);
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<User>(names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_users;
        for (const auto & name : *query.names)
        {
            auto new_user = std::make_shared<User>();
            updateUserFromQueryImpl(*new_user, query, auth_data, name, default_roles_from_query, settings_from_query, RolesOrUsersSet::AllTag{}, implicit_no_password_allowed, no_password_allowed, plaintext_password_allowed);
            new_users.emplace_back(std::move(new_user));
        }

        if (!query.storage_name.empty())
        {
            for (const auto & name : names)
            {
                if (auto another_storage_ptr = access_control.findExcludingStorage(AccessEntityType::USER, name, storage_ptr))
                    throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "User {} already exists in storage {}", name, another_storage_ptr->getStorageName());
            }
        }

        std::vector<UUID> ids;
        if (query.if_not_exists)
            ids = storage->tryInsert(new_users);
        else if (query.or_replace)
            ids = storage->insertOrReplace(new_users);
        else
            ids = storage->insert(new_users);

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
    std::optional<AuthenticationData> auth_data;
    if (query.auth_data)
        auth_data = AuthenticationData::fromAST(*query.auth_data, {}, !query.attach);

    updateUserFromQueryImpl(user, query, auth_data, {}, {}, {}, {}, allow_no_password, allow_plaintext_password, true);
}

}
