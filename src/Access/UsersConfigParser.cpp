#include <Access/UsersConfigParser.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Access/AccessControl.h>
#include <Access/resolveSetting.h>
#include <Access/AccessChangesNotifier.h>
#include <Dictionaries/IDictionary.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/SSHWrapper.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/transformEndianness.h>
#include <Core/Settings.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/MD5Engine.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <cstring>
#include <base/FnTraits.h>
#include <base/range.h>
#include <unordered_set>
#include <boost/container/flat_set.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int THERE_IS_NO_PROFILE;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    template <typename T>
    void parseGrant(T & entity, const String & string_query, const std::unordered_set<UUID> & role_ids_from_users_config, const AccessControl & access_control, LoggerPtr log)
    {
        ParserGrantQuery parser;
        parser.setParseWithoutGrantees();

        String error_message;
        const char * pos = string_query.data();
        auto ast = tryParseQuery(parser, pos, pos + string_query.size(), error_message, false, "", false, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);

        if (!ast)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse grant query. Error: {}", error_message);

        auto & query = ast->as<ASTGrantQuery &>();

        if (query.roles && query.is_revoke)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Roles can't be revoked in config file");

        if (!query.cluster.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't grant on cluster using config file");

        if (query.grantees)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "You can't specify grantees in query using config file");

        for (auto & element : query.access_rights_elements)
        {
            if (query.is_revoke)
                entity.access.revoke(element);
            else
                entity.access.grant(element);
        }

        if (query.roles)
        {
            std::vector<UUID> roles_to_grant;
            roles_to_grant.reserve(query.roles->size());

            for (const auto & role_name : query.roles->names)
            {
                auto role_id = UsersConfigParser::generateID(AccessEntityType::ROLE, role_name);
                if (!role_ids_from_users_config.contains(role_id))
                {
                    if (const auto role = access_control.find<Role>(role_name))
                        role_id = *role;
                    else
                        LOG_WARNING(log, "Role {} is not defined and will be ignored for grant query '{}'.", role_name, string_query);
                }

                roles_to_grant.push_back(role_id);
            }

            if (query.admin_option)
                entity.granted_roles.grantWithAdminOption(roles_to_grant);
            else
                entity.granted_roles.grant(roles_to_grant);
        }
    }

    UserPtr parseUser(
        const Poco::Util::AbstractConfiguration & config,
        String user_name,
        const std::unordered_set<UUID> & allowed_profile_ids,
        const std::unordered_set<UUID> & role_ids_from_users_config,
        const AccessControl & access_control,
        bool allow_no_password,
        bool allow_plaintext_password,
        LoggerPtr log)
    {
        const bool validate = true;
        auto user = std::make_shared<User>();
        String user_config = "users." + user_name;

        /// If the user name contains a dot, it is escaped with a backslash when parsed from the config file.
        /// We need to remove the backslash to get the correct user name.
        Poco::replaceInPlace(user_name, "\\.", ".");
        user->setName(user_name);

        bool has_no_password = config.has(user_config + ".no_password");
        bool has_password_plaintext = config.has(user_config + ".password");
        bool has_password_sha256_hex = config.has(user_config + ".password_sha256_hex");
        bool has_scram_password_sha256_hex = config.has(user_config + ".password_scram_sha256_hex");
        bool has_password_double_sha1_hex = config.has(user_config + ".password_double_sha1_hex");
        bool has_ldap = config.has(user_config + ".ldap");
        bool has_kerberos = config.has(user_config + ".kerberos");

        const auto certificates_config = user_config + ".ssl_certificates";
        bool has_certificates = config.has(certificates_config);

        const auto ssh_keys_config = user_config + ".ssh_keys";
        bool has_ssh_keys = config.has(ssh_keys_config);

        const auto http_auth_config = user_config + ".http_authentication";
        bool has_http_auth = config.has(http_auth_config);

        size_t num_password_fields = has_no_password + has_password_plaintext + has_password_sha256_hex + has_password_double_sha1_hex
            + has_ldap + has_kerberos + has_certificates + has_ssh_keys + has_http_auth + has_scram_password_sha256_hex;

        if (num_password_fields > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "More than one field of 'password', 'password_sha256_hex', "
                            "'password_double_sha1_hex', 'no_password', 'ldap', 'kerberos', 'ssl_certificates', 'ssh_keys', "
                            "'http_authentication' are used to specify authentication info for user {}. "
                            "Must be only one of them.", user_name);

        if (num_password_fields < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Either 'password' or 'password_sha256_hex' "
                            "or 'password_double_sha1_hex' or 'no_password' or 'ldap' or 'kerberos "
                            "or 'ssl_certificates' or 'ssh_keys' or 'http_authentication' must be specified for user {}.", user_name);

        if (has_password_plaintext)
        {
            user->authentication_methods.emplace_back(AuthenticationType::PLAINTEXT_PASSWORD);
            user->authentication_methods.back().setPassword(config.getString(user_config + ".password"), validate);
        }
        else if (has_password_sha256_hex)
        {
            user->authentication_methods.emplace_back(AuthenticationType::SHA256_PASSWORD);
            user->authentication_methods.back().setPasswordHashHex(config.getString(user_config + ".password_sha256_hex"), validate);
        }
        else if (has_scram_password_sha256_hex)
        {
            user->authentication_methods.emplace_back(AuthenticationType::SCRAM_SHA256_PASSWORD);
            user->authentication_methods.back().setPasswordHashHex(config.getString(user_config + ".password_scram_sha256_hex"), validate);
        }
        else if (has_password_double_sha1_hex)
        {
            user->authentication_methods.emplace_back(AuthenticationType::DOUBLE_SHA1_PASSWORD);
            user->authentication_methods.back().setPasswordHashHex(config.getString(user_config + ".password_double_sha1_hex"), validate);
        }
        else if (has_ldap)
        {
            bool has_ldap_server = config.has(user_config + ".ldap.server");
            if (!has_ldap_server)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing mandatory 'server' in 'ldap', with LDAP server name, for user {}.", user_name);

            const auto ldap_server_name = config.getString(user_config + ".ldap.server");
            if (ldap_server_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP server name cannot be empty for user {}.", user_name);

            user->authentication_methods.emplace_back(AuthenticationType::LDAP);
            user->authentication_methods.back().setLDAPServerName(ldap_server_name);
        }
        else if (has_kerberos)
        {
            const auto realm = config.getString(user_config + ".kerberos.realm", "");

            user->authentication_methods.emplace_back(AuthenticationType::KERBEROS);
            user->authentication_methods.back().setKerberosRealm(realm);
        }
        else if (has_certificates)
        {
#if USE_SSL
            user->authentication_methods.emplace_back(AuthenticationType::SSL_CERTIFICATE);

            /// Fill list of allowed certificates.
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(certificates_config, keys);
            for (const String & key : keys)
            {
                if (key.starts_with("common_name"))
                {
                    String value = config.getString(certificates_config + "." + key);
                    user->authentication_methods.back().addSSLCertificateSubject(X509Certificate::Subjects::Type::CN, std::move(value));
                }
                else if (key.starts_with("subject_alt_name"))
                {
                    String value = config.getString(certificates_config + "." + key);
                    if (value.empty())
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected ssl_certificates.subject_alt_name to not be empty");
                    user->authentication_methods.back().addSSLCertificateSubject(X509Certificate::Subjects::Type::SAN, std::move(value));
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown certificate pattern type: {}", key);
            }
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL certificates support is disabled, because ClickHouse was built without SSL library");
#endif
        }
        else if (has_ssh_keys)
        {
#if USE_SSH
            user->authentication_methods.emplace_back(AuthenticationType::SSH_KEY);

            Poco::Util::AbstractConfiguration::Keys entries;
            config.keys(ssh_keys_config, entries);
            std::vector<SSHKey> keys;
            for (const String& entry : entries)
            {
                const auto conf_pref = ssh_keys_config + "." + entry + ".";
                if (entry.starts_with("ssh_key"))
                {
                    String type;
                    String base64_key;
                    if (config.has(conf_pref + "type"))
                    {
                        type = config.getString(conf_pref + "type");
                    }
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected type field in {} entry", entry);
                    if (config.has(conf_pref + "base64_key"))
                    {
                        base64_key = config.getString(conf_pref + "base64_key");
                    }
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected base64_key field in {} entry", entry);


                    try
                    {
                        keys.emplace_back(SSHKeyFactory::makePublicKeyFromBase64(base64_key, type));
                    }
                    catch (const std::invalid_argument &)
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad SSH key in entry: {}", entry);
                    }
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown ssh_key entry pattern type: {}", entry);
            }
            user->authentication_methods.back().setSSHKeys(std::move(keys));
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSH is disabled, because ClickHouse is built without libssh");
#endif
        }
        else if (has_http_auth)
        {
            user->authentication_methods.emplace_back(AuthenticationType::HTTP);
            user->authentication_methods.back().setHTTPAuthenticationServerName(config.getString(http_auth_config + ".server"));
            auto scheme = config.getString(http_auth_config + ".scheme");
            user->authentication_methods.back().setHTTPAuthenticationScheme(parseHTTPAuthenticationScheme(scheme));
        }
        else
        {
            user->authentication_methods.emplace_back();
        }

        for (const auto & authentication_method : user->authentication_methods)
        {
            auto auth_type = authentication_method.getType();
            if (((auth_type == AuthenticationType::NO_PASSWORD) && !allow_no_password) ||
                ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD) && !allow_plaintext_password))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Authentication type {} is not allowed, check the setting allow_{} in the server configuration",
                                toString(auth_type), AuthenticationTypeInfo::get(auth_type).name);
            }
        }

        const auto profile_name_config = user_config + ".profile";
        if (config.has(profile_name_config))
        {
            auto profile_name = config.getString(profile_name_config);
            auto profile_id = UsersConfigParser::generateID(AccessEntityType::SETTINGS_PROFILE, profile_name);
            if (!allowed_profile_ids.contains(profile_id))
                throw Exception(ErrorCodes::THERE_IS_NO_PROFILE, "Profile {} was not found", profile_name);
            SettingsProfileElement profile_element;
            profile_element.parent_profile = profile_id;
            user->settings.push_back(std::move(profile_element));
        }

        /// Fill list of allowed hosts.
        const auto networks_config = user_config + ".networks";
        if (config.has(networks_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(networks_config, keys);
            user->allowed_client_hosts.clear();
            for (const String & key : keys)
            {
                String value = config.getString(networks_config + "." + key);
                if (key.starts_with("ip"))
                    user->allowed_client_hosts.addSubnet(value);
                else if (key.starts_with("host_regexp"))
                    user->allowed_client_hosts.addNameRegexp(value);
                else if (key.starts_with("host"))
                    user->allowed_client_hosts.addName(value);
                else
                    throw Exception(ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE, "Unknown address pattern type: {}", key);
            }
        }

        /// Fill list of allowed databases.
        const auto databases_config = user_config + ".allow_databases";
        std::optional<Strings> databases;
        if (config.has(databases_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(databases_config, keys);
            databases.emplace();
            databases->reserve(keys.size());
            for (const auto & key : keys)
            {
                const auto database_name = config.getString(databases_config + "." + key);
                databases->push_back(database_name);
            }
        }

        /// Fill list of allowed dictionaries.
        const auto dictionaries_config = user_config + ".allow_dictionaries";
        std::optional<Strings> dictionaries;
        if (config.has(dictionaries_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(dictionaries_config, keys);
            dictionaries.emplace();
            dictionaries->reserve(keys.size());
            for (const auto & key : keys)
            {
                const auto dictionary_name = config.getString(dictionaries_config + "." + key);
                dictionaries->push_back(dictionary_name);
            }
        }

        const auto grants_config = user_config + ".grants";
        std::optional<Strings> grant_queries;
        if (config.has(grants_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(grants_config, keys);
            grant_queries.emplace();
            grant_queries->reserve(keys.size());
            for (const auto & key : keys)
            {
                const auto query = config.getString(grants_config + "." + key);
                grant_queries->push_back(query);
            }
        }

        bool access_management = config.getBool(user_config + ".access_management", false);
        bool named_collection_control = config.getBool(user_config + ".named_collection_control", false) || config.getBool(user_config + ".named_collection_admin", false);
        bool show_named_collections_secrets = config.getBool(user_config + ".show_named_collections_secrets", false);

        if (grant_queries)
            if (databases || dictionaries || access_management || named_collection_control || show_named_collections_secrets)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Any other access control settings can't be specified with `grants`");

        if (grant_queries)
        {
            for (const auto & string_query : *grant_queries)
                parseGrant(*user, string_query, role_ids_from_users_config, access_control, log);
        }
        else
        {
            /// By default all databases are accessible
            /// and the user can grant everything he has.
            user->access.grantWithGrantOption(AccessType::ALL);

            if (databases)
            {
                user->access.revoke(AccessFlags::allFlags() - AccessFlags::allGlobalFlags());
                user->access.grantWithGrantOption(AccessType::TABLE_ENGINE);
                user->access.grantWithGrantOption(AccessFlags::allDictionaryFlags(), IDictionary::NO_DATABASE_TAG);
                for (const String & database : *databases)
                    user->access.grantWithGrantOption(AccessFlags::allFlags(), database);
            }

            if (dictionaries)
            {
                user->access.revoke(AccessFlags::allDictionaryFlags(), IDictionary::NO_DATABASE_TAG);
                for (const String & dictionary : *dictionaries)
                    user->access.grantWithGrantOption(AccessFlags::allDictionaryFlags(), IDictionary::NO_DATABASE_TAG, dictionary);
            }

            if (!access_management)
            {
                user->access.revoke(AccessType::ACCESS_MANAGEMENT);
                user->access.revokeGrantOption(AccessType::ALL);
            }

            if (!named_collection_control)
            {
                user->access.revoke(AccessType::NAMED_COLLECTION_ADMIN);
            }

            if (!show_named_collections_secrets)
            {
                user->access.revoke(AccessType::SHOW_NAMED_COLLECTIONS_SECRETS);
            }
        }

        String default_database = config.getString(user_config + ".default_database", "");
        user->default_database = default_database;

        return user;
    }


    RolePtr parseRole(
        const Poco::Util::AbstractConfiguration & config,
        const String & role_name,
        const std::unordered_set<UUID> & role_ids_from_users_config,
        const AccessControl & access_control,
        LoggerPtr log)
    {
        auto role = std::make_shared<Role>();
        role->setName(role_name);

        String role_config = "roles." + role_name;

        const auto grants_config = role_config + ".grants";
        if (config.has(grants_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(grants_config, keys);
            for (const auto & key : keys)
            {
                const auto query = config.getString(grants_config + "." + key);
                parseGrant(*role, query, role_ids_from_users_config, access_control, log);
            }
        }

        return role;
    }


    QuotaPtr parseQuota(const Poco::Util::AbstractConfiguration & config, const String & quota_name, const std::vector<UUID> & user_ids)
    {
        auto quota = std::make_shared<Quota>();
        quota->setName(quota_name);

        String quota_config = "quotas." + quota_name;
        if (config.has(quota_config + ".keyed_by_ip"))
            quota->key_type = QuotaKeyType::IP_ADDRESS;
        else if (config.has(quota_config + ".keyed_by_forwarded_ip"))
            quota->key_type = QuotaKeyType::FORWARDED_IP_ADDRESS;
        else if (config.has(quota_config + ".keyed"))
            quota->key_type = QuotaKeyType::CLIENT_KEY_OR_USER_NAME;
        else
            quota->key_type = QuotaKeyType::USER_NAME;

        Poco::Util::AbstractConfiguration::Keys interval_keys;
        config.keys(quota_config, interval_keys);

        for (const String & interval_key : interval_keys)
        {
            if (!startsWith(interval_key, "interval"))
                continue;

            String interval_config = quota_config + "." + interval_key;
            std::chrono::seconds duration{config.getInt(interval_config + ".duration", 0)};
            if (duration.count() <= 0) /// Skip quotas with non-positive duration.
                continue;

            quota->all_limits.emplace_back();
            auto & limits = quota->all_limits.back();
            limits.duration = duration;
            limits.randomize_interval = config.getBool(interval_config + ".randomize", false);

            for (auto quota_type : collections::range(QuotaType::MAX))
            {
                const auto & type_info = QuotaTypeInfo::get(quota_type);
                auto value = config.getString(interval_config + "." + type_info.name, "0");
                if (value != "0")
                    limits.max[static_cast<size_t>(quota_type)] = type_info.stringToValue(value);
            }
        }

        quota->to_roles.add(user_ids);
        return quota;
    }


    SettingsProfileElements parseSettingsConstraints(const Poco::Util::AbstractConfiguration & config,
                                                     const String & path_to_constraints,
                                                     const AccessControl & access_control)
    {
        SettingsProfileElements profile_elements;
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(path_to_constraints, keys);

        for (const String & setting_name : keys)
        {
            access_control.checkSettingNameIsAllowed(setting_name);

            SettingsProfileElement profile_element;
            profile_element.setting_name = setting_name;
            Poco::Util::AbstractConfiguration::Keys constraint_types;
            String path_to_name = path_to_constraints + "." + setting_name;
            config.keys(path_to_name, constraint_types);

            size_t writability_count = 0;
            for (const String & constraint_type : constraint_types)
            {
                if (constraint_type == "min")
                    profile_element.min_value = settingStringToValueUtil(setting_name, config.getString(path_to_name + "." + constraint_type));
                else if (constraint_type == "max")
                    profile_element.max_value = settingStringToValueUtil(setting_name, config.getString(path_to_name + "." + constraint_type));
                /// When the xml config is parsed, the first constraint_type is parsed as `disallowed` and the subsequent ones are parsed as
                /// disallowed[1], disallowed[2] and so on. So, both `disallowed` and `disallowed[` should be considered as valid constraint types.
                /// Example:
                ///   <constraints>
                ///     <max_execution_time>
                ///         <max>50</max>
                ///         <disallowed>3</disallowed>
                ///         <disallowed>4</disallowed>
                ///         <disallowed>5</disallowed>
                ///     </max_execution_time>
                /// </constraints>
                else if (constraint_type == "disallowed" || constraint_type.starts_with("disallowed["))
                    profile_element.disallowed_values.push_back(settingStringToValueUtil(setting_name, config.getString(path_to_name + "." + constraint_type)));
                else if (constraint_type == "readonly" || constraint_type == "const")
                {
                    writability_count++;
                    profile_element.writability = SettingConstraintWritability::CONST;
                }
                else if (constraint_type == "changeable_in_readonly")
                {
                    writability_count++;
                    if (access_control.doesSettingsConstraintsReplacePrevious())
                        profile_element.writability = SettingConstraintWritability::CHANGEABLE_IN_READONLY;
                    else
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Setting changeable_in_readonly for {} is not allowed "
                                        "unless settings_constraints_replace_previous is enabled", setting_name);
                }
                else
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Setting {} value for {} isn't supported", constraint_type, setting_name);
            }
            if (writability_count > 1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not more than one constraint writability specifier "
                                "(const/readonly/changeable_in_readonly) is allowed for {}", setting_name);

            profile_elements.push_back(std::move(profile_element));
        }

        return profile_elements;
    }

    std::shared_ptr<SettingsProfile> parseSettingsProfile(
        const Poco::Util::AbstractConfiguration & config,
        const String & profile_name,
        const std::unordered_set<UUID> & allowed_parent_profile_ids,
        const AccessControl & access_control)
    {
        auto profile = std::make_shared<SettingsProfile>();
        profile->setName(profile_name);
        String profile_config = "profiles." + profile_name;

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(profile_config, keys);

        for (const std::string & key : keys)
        {
            if (key == "profile" || key.starts_with("profile["))
            {
                String parent_profile_name = config.getString(profile_config + "." + key);
                auto parent_profile_id = UsersConfigParser::generateID(AccessEntityType::SETTINGS_PROFILE, parent_profile_name);
                if (!allowed_parent_profile_ids.contains(parent_profile_id))
                    throw Exception(ErrorCodes::THERE_IS_NO_PROFILE, "Parent profile '{}' was not found", parent_profile_name);
                SettingsProfileElement profile_element;
                profile_element.parent_profile = parent_profile_id;
                profile->elements.emplace_back(std::move(profile_element));
                continue;
            }

            if (key == "constraints" || key.starts_with("constraints["))
            {
                profile->elements.merge(parseSettingsConstraints(config, profile_config + "." + key, access_control));
                continue;
            }

            const auto & setting_name = key;
            access_control.checkSettingNameIsAllowed(setting_name);

            SettingsProfileElement profile_element;
            profile_element.setting_name = setting_name;
            profile_element.value = settingStringToValueUtil(setting_name, config.getString(profile_config + "." + key));
            profile->elements.emplace_back(std::move(profile_element));
        }

        return profile;
    }
}


UsersConfigParser::UsersConfigParser(AccessControl & access_control_)
    : access_control(access_control_)
    , log(getLogger("UsersConfigParser"))
{
}

UUID UsersConfigParser::generateID(AccessEntityType type, const String & name)
{
    Poco::MD5Engine md5;
    md5.update(name);
    char type_storage_chars[] = " USRSXML";
    type_storage_chars[0] = AccessEntityTypeInfo::get(type).unique_char;
    md5.update(type_storage_chars, strlen(type_storage_chars));
    UUID result;
    memcpy(&result, md5.digest().data(), md5.digestLength());
    transformEndianness<std::endian::native, std::endian::little>(result);
    return result;
}

UUID UsersConfigParser::generateID(const IAccessEntity & entity) { return generateID(entity.getType(), entity.getName()); }


std::vector<AccessEntityPtr> UsersConfigParser::parseUsers(
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_set<UUID> & allowed_profile_ids,
    const std::unordered_set<UUID> & role_ids_from_users_config) const
{
    Poco::Util::AbstractConfiguration::Keys user_names;
    config.keys("users", user_names);

    bool no_password_allowed = access_control.isNoPasswordAllowed();
    bool plaintext_password_allowed = access_control.isPlaintextPasswordAllowed();

    std::vector<AccessEntityPtr> users;
    users.reserve(user_names.size());
    for (const auto & user_name : user_names)
    {
        try
        {
            users.push_back(parseUser(config, user_name, allowed_profile_ids, role_ids_from_users_config, access_control, no_password_allowed, plaintext_password_allowed, log));
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while parsing user '{}' in users configuration file", user_name));
            throw;
        }
    }

    return users;
}

std::vector<AccessEntityPtr> UsersConfigParser::parseRoles(
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_set<UUID> & role_ids_from_users_config) const
{
    Poco::Util::AbstractConfiguration::Keys role_names;
    config.keys("roles", role_names);

    std::vector<AccessEntityPtr> roles;
    roles.reserve(role_names.size());
    for (const auto & role_name : role_names)
    {
        try
        {
            roles.push_back(parseRole(config, role_name, role_ids_from_users_config, access_control, log));
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while parsing roles '{}' in users configuration file", role_name));
            throw;
        }
    }

    return roles;
}


std::vector<AccessEntityPtr> UsersConfigParser::parseQuotas(const Poco::Util::AbstractConfiguration & config) const
{
    Poco::Util::AbstractConfiguration::Keys user_names;
    config.keys("users", user_names);
    std::unordered_map<String, std::vector<UUID>> quota_to_user_ids;
    for (const auto & user_name : user_names)
    {
        if (config.has("users." + user_name + ".quota"))
            quota_to_user_ids[config.getString("users." + user_name + ".quota")].push_back(generateID(AccessEntityType::USER, user_name));
    }

    Poco::Util::AbstractConfiguration::Keys quota_names;
    config.keys("quotas", quota_names);

    std::vector<AccessEntityPtr> quotas;
    quotas.reserve(quota_names.size());

    for (const auto & quota_name : quota_names)
    {
        try
        {
            auto it = quota_to_user_ids.find(quota_name);
            const std::vector<UUID> & quota_users = (it != quota_to_user_ids.end()) ? std::move(it->second) : std::vector<UUID>{};
            quotas.push_back(parseQuota(config, quota_name, quota_users));
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while parsing quota '{}' in users configuration file", quota_name));
            throw;
        }
    }

    return quotas;
}


std::vector<AccessEntityPtr> UsersConfigParser::parseRowPolicies(const Poco::Util::AbstractConfiguration & config) const
{
    bool users_without_row_policies_can_read_rows = access_control.isEnabledUsersWithoutRowPoliciesCanReadRows();
    std::map<std::pair<String /* database */, String /* table */>, std::unordered_map<String /* user */, String /* filter */>> all_filters_map;

    Poco::Util::AbstractConfiguration::Keys user_names;
    config.keys("users", user_names);

    for (const String & user_name : user_names)
    {
        const String databases_config = "users." + user_name + ".databases";
        if (config.has(databases_config))
        {
            Poco::Util::AbstractConfiguration::Keys database_keys;
            config.keys(databases_config, database_keys);

            /// Read tables within databases
            for (const String & database_key : database_keys)
            {
                const String database_config = databases_config + "." + database_key;

                String database_name;
                if (((database_key == "database") || (database_key.starts_with("database["))) && config.has(database_config + "[@name]"))
                    database_name = config.getString(database_config + "[@name]");
                else if (size_t bracket_pos = database_key.find('['); bracket_pos != std::string::npos)
                    database_name = database_key.substr(0, bracket_pos);
                else
                    database_name = database_key;

                Poco::Util::AbstractConfiguration::Keys table_keys;
                config.keys(database_config, table_keys);

                /// Read table properties
                for (const String & table_key : table_keys)
                {
                    String table_config = database_config + "." + table_key;
                    String table_name;
                    if (((table_key == "table") || (table_key.starts_with("table["))) && config.has(table_config + "[@name]"))
                        table_name = config.getString(table_config + "[@name]");
                    else if (size_t bracket_pos = table_key.find('['); bracket_pos != std::string::npos)
                        table_name = table_key.substr(0, bracket_pos);
                    else
                        table_name = table_key;

                    String filter_config = table_config + ".filter";
                    all_filters_map[{database_name, table_name}][user_name] = config.getString(filter_config);
                }
            }
        }
    }

    std::vector<AccessEntityPtr> policies;
    for (auto & [database_and_table_name, user_to_filters] : all_filters_map)
    {
        const auto & [database, table_name] = database_and_table_name;
        for (const String & user_name : user_names)
        {
            String filter;
            auto it = user_to_filters.find(user_name);
            if (it != user_to_filters.end())
            {
                filter = it->second;
            }
            else
            {
                if (users_without_row_policies_can_read_rows)
                    continue;
                filter = "1";
            }

            auto policy = std::make_shared<RowPolicy>();
            policy->setFullName(user_name, database, table_name);
            policy->filters[static_cast<size_t>(RowPolicyFilterType::SELECT_FILTER)] = filter;
            policy->to_roles.add(generateID(AccessEntityType::USER, user_name));
            policies.push_back(policy);
        }
    }
    return policies;
}


std::vector<AccessEntityPtr> UsersConfigParser::parseSettingsProfiles(
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_set<UUID> & allowed_parent_profile_ids) const
{
    Poco::Util::AbstractConfiguration::Keys profile_names;
    config.keys("profiles", profile_names);

    std::vector<AccessEntityPtr> profiles;
    profiles.reserve(profile_names.size());

    for (const auto & profile_name : profile_names)
    {
        try
        {
            profiles.push_back(parseSettingsProfile(config, profile_name, allowed_parent_profile_ids, access_control));
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while parsing profile '{}' in users configuration file", profile_name));
            throw;
        }
    }

    return profiles;
}

std::unordered_set<UUID> UsersConfigParser::getAllowedIDs(
    const Poco::Util::AbstractConfiguration & config,
    const String & configuration_key,
    AccessEntityType type)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(configuration_key, keys);
    std::unordered_set<UUID> ids;
    for (const auto & key : keys)
        ids.emplace(generateID(type, key));
    return ids;
}

}
