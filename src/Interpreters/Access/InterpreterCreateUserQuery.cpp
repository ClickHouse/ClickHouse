#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <boost/range/algorithm/copy.hpp>

#include "config.h"

#if USE_SSL
#     include <openssl/crypto.h>
#     include <openssl/rand.h>
#     include <openssl/err.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int OPENSSL_ERROR;
    extern const int LOGICAL_ERROR;
}
namespace
{
    AuthenticationData makeAuthenticationData(const ASTAuthenticationData & query, ContextPtr context, bool check_password_rules)
    {
        if (query.type && *query.type == AuthenticationType::NO_PASSWORD)
            return AuthenticationData();

        size_t args_size = query.children.size();
        ASTs args(args_size);
        for (size_t i = 0; i < args_size; ++i)
            args[i] = evaluateConstantExpressionAsLiteral(query.children[i], context);

        if (query.expect_password)
        {
            if (!query.type && !context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get default password type without context");

            if (check_password_rules && !context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot check password complexity rules without context");

            /// NOTE: We will also extract bcrypt workfactor from context

            String value = checkAndGetLiteralArgument<String>(args[0], "password");

            AuthenticationType current_type;

            if (query.type)
                current_type = *query.type;
            else
                current_type = context->getAccessControl().getDefaultPasswordType();

            AuthenticationData auth_data(current_type);

            if (check_password_rules)
                context->getAccessControl().checkPasswordComplexityRules(value);

            if (query.type == AuthenticationType::SHA256_PASSWORD)
            {
    #if USE_SSL
                ///random generator FIPS complaint
                uint8_t key[32];
                if (RAND_bytes(key, sizeof(key)) != 1)
                {
                    char buf[512] = {0};
                    ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
                    throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot generate salt for password. OpenSSL {}", buf);
                }

                String salt;
                salt.resize(sizeof(key) * 2);
                char * buf_pos = salt.data();
                for (uint8_t k : key)
                {
                    writeHexByteUppercase(k, buf_pos);
                    buf_pos += 2;
                }
                value.append(salt);
                auth_data.setSalt(salt);
    #else
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                "SHA256 passwords support is disabled, because ClickHouse was built without SSL library");
    #endif
            }

            auth_data.setPassword(value);
            return auth_data;
        }

        AuthenticationData auth_data(*query.type);

        if (query.expect_hash)
        {
            String value = checkAndGetLiteralArgument<String>(args[0], "hash");
            auth_data.setPasswordHashHex(value);

            if (*query.type == AuthenticationType::SHA256_PASSWORD && args_size == 2)
            {
                String parsed_salt = checkAndGetLiteralArgument<String>(args[1], "salt");
                auth_data.setSalt(parsed_salt);
            }
        }
        else if (query.expect_ldap_server_name)
        {
            String value = checkAndGetLiteralArgument<String>(args[0], "ldap_server_name");
            auth_data.setLDAPServerName(value);
        }
        else if (query.expect_kerberos_realm)
        {
            if (!args.empty())
            {
                String value = checkAndGetLiteralArgument<String>(args[0], "kerberos_realm");
                auth_data.setKerberosRealm(value);
            }
        }
        else if (query.expect_common_names)
        {
            boost::container::flat_set<String> common_names;
            for (const auto & arg : args)
                common_names.insert(checkAndGetLiteralArgument<String>(arg, "common_name"));

            auth_data.setSSLCertificateCommonNames(std::move(common_names));
        }

        return auth_data;
    }

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
    const auto & query = query_ptr->as<const ASTCreateUserQuery &>();
    auto & access_control = getContext()->getAccessControl();
    auto access = getContext()->getAccess();
    access->checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);
    bool implicit_no_password_allowed = access_control.isImplicitNoPasswordAllowed();
    bool no_password_allowed = access_control.isNoPasswordAllowed();
    bool plaintext_password_allowed = access_control.isPlaintextPasswordAllowed();

    std::optional<AuthenticationData> auth_data;
    if (query.auth_data)
        auth_data = makeAuthenticationData(*query.auth_data, getContext(), !query.attach);

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
            getContext()->checkSettingsConstraints(*settings_from_query);
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

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
            updateUserFromQueryImpl(*new_user, query, auth_data, name, default_roles_from_query, settings_from_query, RolesOrUsersSet::AllTag{}, implicit_no_password_allowed, no_password_allowed, plaintext_password_allowed);
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
    std::optional<AuthenticationData> auth_data;
    if (query.auth_data)
        auth_data = makeAuthenticationData(*query.auth_data, {}, !query.attach);

    updateUserFromQueryImpl(user, query, auth_data, {}, {}, {}, {}, allow_no_password, allow_plaintext_password, true);
}

}
