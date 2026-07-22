#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateUserQuery.h>

#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/User.h>
#include <Common/logger_useful.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Interpreters/Access/getValidUntilFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <boost/range/algorithm/copy.hpp>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{
namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_authentication_methods_per_user;
}

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
        const std::vector<AuthenticationData> authentication_methods,
        const std::shared_ptr<ASTUserNameWithHost> & override_name,
        const std::optional<RolesOrUsersSet> & override_default_roles,
        const std::optional<AlterSettingsProfileElements> & override_settings,
        const std::optional<RolesOrUsersSet> & override_grantees,
        const std::optional<time_t> & global_valid_until,
        bool reset_authentication_methods,
        bool replace_authentication_methods,
        bool allow_implicit_no_password,
        bool allow_no_password,
        bool allow_plaintext_password,
        std::size_t max_number_of_authentication_methods)
    {
        if (override_name)
            user.setName(override_name->toString());
        else if (query.new_name)
            user.setName(*query.new_name);
        else if (query.names->size() == 1)
            user.setName(query.names->toStrings().at(0));

        if (!query.attach && !query.alter && authentication_methods.empty() && !allow_implicit_no_password)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Authentication type NO_PASSWORD must "
                            "be explicitly specified, check the setting allow_implicit_no_password "
                            "in the server configuration");

        // if user does not have an authentication method and it has not been specified in the query,
        // add a default one
        if (user.authentication_methods.empty() && authentication_methods.empty())
        {
            user.authentication_methods.emplace_back();
        }

        // 1. an IDENTIFIED WITH will drop existing authentication methods in favor of new ones.
        if (replace_authentication_methods)
        {
            user.authentication_methods.clear();
        }

        // drop existing ones and keep the most recent
        if (reset_authentication_methods)
        {
            auto backup_authentication_method = user.authentication_methods.back();
            user.authentication_methods.clear();
            user.authentication_methods.emplace_back(backup_authentication_method);
        }

        // max_number_of_authentication_methods == 0 means unlimited
        if (!authentication_methods.empty() && max_number_of_authentication_methods != 0)
        {
            // we only check if user exceeds the allowed quantity of authentication methods in case the create/alter query includes
            // authentication information. Otherwise, we can bypass this check to avoid blocking non-authentication related alters.
            auto number_of_authentication_methods = user.authentication_methods.size() + authentication_methods.size();
            if (number_of_authentication_methods > max_number_of_authentication_methods)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user. "
                                "Check the `max_authentication_methods_per_user` setting");
            }
        }

        for (const auto & authentication_method : authentication_methods)
        {
            user.authentication_methods.emplace_back(authentication_method);
        }

        bool has_no_password_authentication_method = false;

        for (auto & authentication_method : user.authentication_methods)
        {
            if (global_valid_until)
            {
                authentication_method.setValidUntil(*global_valid_until);
            }

            if (authentication_method.getType() == AuthenticationType::NO_PASSWORD)
            {
                has_no_password_authentication_method = true;
            }
        }

        if (has_no_password_authentication_method && user.authentication_methods.size() > 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Authentication method 'no_password' cannot co-exist with other authentication methods");
        }

        if (!query.alter)
        {
            for (const auto & authentication_method : user.authentication_methods)
            {
                auto auth_type = authentication_method.getType();
                if (((auth_type == AuthenticationType::NO_PASSWORD) && !allow_no_password) ||
                    ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD)  && !allow_plaintext_password))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Authentication type {} is not allowed, check the setting allow_{} in the server configuration",
                                    toString(auth_type),
                                    AuthenticationTypeInfo::get(auth_type).name);
                }
            }
        }

        if (override_name && !override_name->getHostPattern().empty())
        {
            user.allowed_client_hosts = AllowedClientHosts{};
            user.allowed_client_hosts.addLikePattern(override_name->getHostPattern());
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
            user.settings.applyChanges(*override_settings);
        else if (query.alter_settings)
            user.settings.applyChanges(AlterSettingsProfileElements{*query.alter_settings});
        else if (query.settings)
            user.settings.applyChanges(AlterSettingsProfileElements{*query.settings});

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

    for (const auto & name : query.names->toStrings())
        access->checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER, name);

    if (query.new_name && !query.alter)
        access->checkAccess(AccessType::CREATE_USER, *query.new_name);

    bool implicit_no_password_allowed = access_control.isImplicitNoPasswordAllowed();
    bool no_password_allowed = access_control.isNoPasswordAllowed();
    bool plaintext_password_allowed = access_control.isPlaintextPasswordAllowed();

    std::vector<AuthenticationData> authentication_methods;
    if (!query.authentication_methods.empty())
    {
        for (const auto & authentication_method_ast : query.authentication_methods)
        {
            authentication_methods.push_back(AuthenticationData::fromAST(*authentication_method_ast, getContext(), !query.attach));
        }
    }

    std::optional<time_t> global_valid_until;
    if (query.global_valid_until)
        global_valid_until = getValidUntilFromAST(query.global_valid_until, getContext());

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

    std::optional<AlterSettingsProfileElements> settings_from_query;
    if (query.alter_settings)
        settings_from_query = AlterSettingsProfileElements{*query.alter_settings, access_control};
    else if (query.settings)
        settings_from_query = AlterSettingsProfileElements{*query.settings, access_control};

    if (settings_from_query && !query.attach)
        getContext()->checkSettingsConstraints(*settings_from_query, SettingSource::USER);

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

        auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
        {
            auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
            updateUserFromQueryImpl(
                *updated_user, query, authentication_methods, {}, default_roles_from_query, settings_from_query, grantees_from_query,
                global_valid_until, query.reset_authentication_methods_to_new, query.replace_authentication_methods,
                implicit_no_password_allowed, no_password_allowed,
                plaintext_password_allowed, getContext()->getServerSettings()[ServerSetting::max_authentication_methods_per_user]);
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
            const auto & name_with_host = typeid_cast<std::shared_ptr<ASTUserNameWithHost>>(name);
            updateUserFromQueryImpl(
                *new_user, query, authentication_methods, name_with_host, default_roles_from_query, settings_from_query, RolesOrUsersSet::AllTag{},
                global_valid_until, query.reset_authentication_methods_to_new, query.replace_authentication_methods,
                implicit_no_password_allowed, no_password_allowed,
                plaintext_password_allowed, getContext()->getServerSettings()[ServerSetting::max_authentication_methods_per_user]);
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
            access_control.update(ids, [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
            {
                auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
                updated_user->grantees = grantees_from_query;
                return updated_user;
            });
        }
    }

    return {};
}


void InterpreterCreateUserQuery::updateUserFromQuery(
    User & user,
    const ASTCreateUserQuery & query,
    bool allow_no_password,
    bool allow_plaintext_password,
    std::size_t max_number_of_authentication_methods)
{
    std::vector<AuthenticationData> authentication_methods;
    if (!query.authentication_methods.empty())
    {
        for (const auto & authentication_method_ast : query.authentication_methods)
        {
            authentication_methods.emplace_back(AuthenticationData::fromAST(*authentication_method_ast, {}, !query.attach));
        }
    }

    std::optional<time_t> global_valid_until;
    if (query.global_valid_until)
        global_valid_until = getValidUntilFromAST(query.global_valid_until, {});

    updateUserFromQueryImpl(
        user,
        query,
        authentication_methods,
        {},
        {},
        {},
        {},
        global_valid_until,
        query.reset_authentication_methods_to_new,
        query.replace_authentication_methods,
        allow_no_password,
        allow_plaintext_password,
        true,
        max_number_of_authentication_methods);
}

void registerInterpreterCreateUserQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateUserQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateUserQuery", create_fn);
}

}
