#include <Access/UsersConfigAccessStorage.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <Access/SettingsProfile.h>
#include <Dictionaries/IDictionary.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/MD5Engine.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <base/logger_useful.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/map.hpp>
#include <cstring>
#include <filesystem>
#include <base/FnTraits.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

    UUID generateID(AccessEntityType type, const String & name)
    {
        Poco::MD5Engine md5;
        md5.update(name);
        char type_storage_chars[] = " USRSXML";
        type_storage_chars[0] = AccessEntityTypeInfo::get(type).unique_char;
        md5.update(type_storage_chars, strlen(type_storage_chars));
        UUID result;
        memcpy(&result, md5.digest().data(), md5.digestLength());
        return result;
    }

    UUID generateID(const IAccessEntity & entity) { return generateID(entity.getType(), entity.getName()); }


    UserPtr parseUser(const Poco::Util::AbstractConfiguration & config, const String & user_name, bool allow_no_password, bool allow_plaintext_password)
    {
        auto user = std::make_shared<User>();
        user->setName(user_name);
        String user_config = "users." + user_name;
        bool has_no_password = config.has(user_config + ".no_password");
        bool has_password_plaintext = config.has(user_config + ".password");
        bool has_password_sha256_hex = config.has(user_config + ".password_sha256_hex");
        bool has_password_double_sha1_hex = config.has(user_config + ".password_double_sha1_hex");
        bool has_ldap = config.has(user_config + ".ldap");
        bool has_kerberos = config.has(user_config + ".kerberos");

        const auto certificates_config = user_config + ".ssl_certificates";
        bool has_certificates = config.has(certificates_config);

        size_t num_password_fields = has_no_password + has_password_plaintext + has_password_sha256_hex + has_password_double_sha1_hex + has_ldap + has_kerberos + has_certificates;

        if (num_password_fields > 1)
            throw Exception("More than one field of 'password', 'password_sha256_hex', 'password_double_sha1_hex', 'no_password', 'ldap', 'kerberos', 'certificates' are used to specify authentication info for user " + user_name + ". Must be only one of them.",
                ErrorCodes::BAD_ARGUMENTS);

        if (num_password_fields < 1)
            throw Exception("Either 'password' or 'password_sha256_hex' or 'password_double_sha1_hex' or 'no_password' or 'ldap' or 'kerberos' or 'certificates' must be specified for user " + user_name + ".", ErrorCodes::BAD_ARGUMENTS);

        if (has_password_plaintext)
        {
            user->auth_data = AuthenticationData{AuthenticationType::PLAINTEXT_PASSWORD};
            user->auth_data.setPassword(config.getString(user_config + ".password"));
        }
        else if (has_password_sha256_hex)
        {
            user->auth_data = AuthenticationData{AuthenticationType::SHA256_PASSWORD};
            user->auth_data.setPasswordHashHex(config.getString(user_config + ".password_sha256_hex"));
        }
        else if (has_password_double_sha1_hex)
        {
            user->auth_data = AuthenticationData{AuthenticationType::DOUBLE_SHA1_PASSWORD};
            user->auth_data.setPasswordHashHex(config.getString(user_config + ".password_double_sha1_hex"));
        }
        else if (has_ldap)
        {
            bool has_ldap_server = config.has(user_config + ".ldap.server");
            if (!has_ldap_server)
                throw Exception("Missing mandatory 'server' in 'ldap', with LDAP server name, for user " + user_name + ".", ErrorCodes::BAD_ARGUMENTS);

            const auto ldap_server_name = config.getString(user_config + ".ldap.server");
            if (ldap_server_name.empty())
                throw Exception("LDAP server name cannot be empty for user " + user_name + ".", ErrorCodes::BAD_ARGUMENTS);

            user->auth_data = AuthenticationData{AuthenticationType::LDAP};
            user->auth_data.setLDAPServerName(ldap_server_name);
        }
        else if (has_kerberos)
        {
            const auto realm = config.getString(user_config + ".kerberos.realm", "");

            user->auth_data = AuthenticationData{AuthenticationType::KERBEROS};
            user->auth_data.setKerberosRealm(realm);
        }
        else if (has_certificates)
        {
            user->auth_data = AuthenticationData{AuthenticationType::SSL_CERTIFICATE};

            /// Fill list of allowed certificates.
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(certificates_config, keys);
            boost::container::flat_set<String> common_names;
            for (const String & key : keys)
            {
                if (key.starts_with("common_name"))
                {
                    String value = config.getString(certificates_config + "." + key);
                    common_names.insert(std::move(value));
                }
                else
                    throw Exception("Unknown certificate pattern type: " + key, ErrorCodes::BAD_ARGUMENTS);
            }
            user->auth_data.setSSLCertificateCommonNames(std::move(common_names));
        }

        auto auth_type = user->auth_data.getType();
        if (((auth_type == AuthenticationType::NO_PASSWORD) && !allow_no_password) ||
            ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD) && !allow_plaintext_password))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Authentication type {} is not allowed, check the setting allow_{} in the server configuration",
                            toString(auth_type), AuthenticationTypeInfo::get(auth_type).name);
        }

        const auto profile_name_config = user_config + ".profile";
        if (config.has(profile_name_config))
        {
            auto profile_name = config.getString(profile_name_config);
            SettingsProfileElement profile_element;
            profile_element.parent_profile = generateID(AccessEntityType::SETTINGS_PROFILE, profile_name);
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
                    throw Exception("Unknown address pattern type: " + key, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);
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

        /// By default all databases are accessible
        /// and the user can grant everything he has.
        user->access.grantWithGrantOption(AccessType::ALL);

        if (databases)
        {
            user->access.revoke(AccessFlags::allFlags() - AccessFlags::allGlobalFlags());
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

        bool access_management = config.getBool(user_config + ".access_management", false);
        if (!access_management)
        {
            user->access.revoke(AccessType::ACCESS_MANAGEMENT);
            user->access.revokeGrantOption(AccessType::ALL);
        }

        String default_database = config.getString(user_config + ".default_database", "");
        user->default_database = default_database;

        return user;
    }


    std::vector<AccessEntityPtr> parseUsers(const Poco::Util::AbstractConfiguration & config, bool allow_no_password, bool allow_plaintext_password)
    {
        Poco::Util::AbstractConfiguration::Keys user_names;
        config.keys("users", user_names);

        std::vector<AccessEntityPtr> users;
        users.reserve(user_names.size());
        for (const auto & user_name : user_names)
        {
            try
            {
                users.push_back(parseUser(config, user_name, allow_no_password, allow_plaintext_password));
            }
            catch (Exception & e)
            {
                e.addMessage(fmt::format("while parsing user '{}' in users configuration file", user_name));
                throw;
            }
        }

        return users;
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


    std::vector<AccessEntityPtr> parseQuotas(const Poco::Util::AbstractConfiguration & config)
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


    std::vector<AccessEntityPtr> parseRowPolicies(const Poco::Util::AbstractConfiguration & config)
    {
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
                auto it = user_to_filters.find(user_name);
                String filter = (it != user_to_filters.end()) ? it->second : "1";

                auto policy = std::make_shared<RowPolicy>();
                policy->setFullName(user_name, database, table_name);
                policy->filters[static_cast<size_t>(RowPolicyFilterType::SELECT_FILTER)] = filter;
                policy->to_roles.add(generateID(AccessEntityType::USER, user_name));
                policies.push_back(policy);
            }
        }
        return policies;
    }


    SettingsProfileElements parseSettingsConstraints(const Poco::Util::AbstractConfiguration & config,
                                                     const String & path_to_constraints,
                                                     Fn<void(std::string_view)> auto && check_setting_name_function)
    {
        SettingsProfileElements profile_elements;
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(path_to_constraints, keys);

        for (const String & setting_name : keys)
        {
            if (check_setting_name_function)
                check_setting_name_function(setting_name);

            SettingsProfileElement profile_element;
            profile_element.setting_name = setting_name;
            Poco::Util::AbstractConfiguration::Keys constraint_types;
            String path_to_name = path_to_constraints + "." + setting_name;
            config.keys(path_to_name, constraint_types);

            for (const String & constraint_type : constraint_types)
            {
                if (constraint_type == "min")
                    profile_element.min_value = Settings::stringToValueUtil(setting_name, config.getString(path_to_name + "." + constraint_type));
                else if (constraint_type == "max")
                    profile_element.max_value = Settings::stringToValueUtil(setting_name, config.getString(path_to_name + "." + constraint_type));
                else if (constraint_type == "readonly")
                    profile_element.readonly = true;
                else
                    throw Exception("Setting " + constraint_type + " value for " + setting_name + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
            }
            profile_elements.push_back(std::move(profile_element));
        }

        return profile_elements;
    }

    std::shared_ptr<SettingsProfile> parseSettingsProfile(
        const Poco::Util::AbstractConfiguration & config,
        const String & profile_name,
        Fn<void(std::string_view)> auto && check_setting_name_function)
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
                SettingsProfileElement profile_element;
                profile_element.parent_profile = generateID(AccessEntityType::SETTINGS_PROFILE, parent_profile_name);
                profile->elements.emplace_back(std::move(profile_element));
                continue;
            }

            if (key == "constraints" || key.starts_with("constraints["))
            {
                profile->elements.merge(parseSettingsConstraints(config, profile_config + "." + key, check_setting_name_function));
                continue;
            }

            const auto & setting_name = key;
            if (check_setting_name_function)
                check_setting_name_function(setting_name);

            SettingsProfileElement profile_element;
            profile_element.setting_name = setting_name;
            profile_element.value = Settings::stringToValueUtil(setting_name, config.getString(profile_config + "." + key));
            profile->elements.emplace_back(std::move(profile_element));
        }

        return profile;
    }


    std::vector<AccessEntityPtr> parseSettingsProfiles(
        const Poco::Util::AbstractConfiguration & config,
        Fn<void(std::string_view)> auto && check_setting_name_function)
    {
        Poco::Util::AbstractConfiguration::Keys profile_names;
        config.keys("profiles", profile_names);

        std::vector<AccessEntityPtr> profiles;
        profiles.reserve(profile_names.size());

        for (const auto & profile_name : profile_names)
        {
            try
            {
                profiles.push_back(parseSettingsProfile(config, profile_name, check_setting_name_function));
            }
            catch (Exception & e)
            {
                e.addMessage(fmt::format("while parsing profile '{}' in users configuration file", profile_name));
                throw;
            }
        }

        return profiles;
    }
}

UsersConfigAccessStorage::UsersConfigAccessStorage(const CheckSettingNameFunction & check_setting_name_function_, const IsNoPasswordFunction & is_no_password_allowed_function_, const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_)
    : UsersConfigAccessStorage(STORAGE_TYPE, check_setting_name_function_, is_no_password_allowed_function_, is_plaintext_password_allowed_function_)
{
}

UsersConfigAccessStorage::UsersConfigAccessStorage(const String & storage_name_, const CheckSettingNameFunction & check_setting_name_function_, const IsNoPasswordFunction & is_no_password_allowed_function_, const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_)
    : IAccessStorage(storage_name_), check_setting_name_function(check_setting_name_function_),is_no_password_allowed_function(is_no_password_allowed_function_), is_plaintext_password_allowed_function(is_plaintext_password_allowed_function_)
{
}

UsersConfigAccessStorage::~UsersConfigAccessStorage() = default;


String UsersConfigAccessStorage::getStorageParamsJSON() const
{
    std::lock_guard lock{load_mutex};
    Poco::JSON::Object json;
    if (!path.empty())
        json.set("path", path);
    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

String UsersConfigAccessStorage::getPath() const
{
    std::lock_guard lock{load_mutex};
    return path;
}

bool UsersConfigAccessStorage::isPathEqual(const String & path_) const
{
    return getPath() == path_;
}

void UsersConfigAccessStorage::setConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock{load_mutex};
    path.clear();
    config_reloader.reset();
    parseFromConfig(config);
}

void UsersConfigAccessStorage::parseFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    try
    {
        bool no_password_allowed = is_no_password_allowed_function();
        bool plaintext_password_allowed = is_plaintext_password_allowed_function();
        std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
        for (const auto & entity : parseUsers(config, no_password_allowed, plaintext_password_allowed))
            all_entities.emplace_back(generateID(*entity), entity);
        for (const auto & entity : parseQuotas(config))
            all_entities.emplace_back(generateID(*entity), entity);
        for (const auto & entity : parseRowPolicies(config))
            all_entities.emplace_back(generateID(*entity), entity);
        for (const auto & entity : parseSettingsProfiles(config, check_setting_name_function))
            all_entities.emplace_back(generateID(*entity), entity);
        memory_storage.setAll(all_entities);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading {}", path.empty() ? "configuration" : ("configuration file " + quoteString(path))));
        throw;
    }
}

void UsersConfigAccessStorage::load(
    const String & users_config_path,
    const String & include_from_path,
    const String & preprocessed_dir,
    const zkutil::GetZooKeeper & get_zookeeper_function)
{
    std::lock_guard lock{load_mutex};
    path = std::filesystem::path{users_config_path}.lexically_normal();
    config_reloader.reset();
    config_reloader = std::make_unique<ConfigReloader>(
        users_config_path,
        include_from_path,
        preprocessed_dir,
        zkutil::ZooKeeperNodeCache(get_zookeeper_function),
        std::make_shared<Poco::Event>(),
        [&](Poco::AutoPtr<Poco::Util::AbstractConfiguration> new_config, bool /*initial_loading*/)
        {
            parseFromConfig(*new_config);

            Settings::checkNoSettingNamesAtTopLevel(*new_config, users_config_path);
        },
        /* already_loaded = */ false);
}

void UsersConfigAccessStorage::reload()
{
    std::lock_guard lock{load_mutex};
    if (config_reloader)
        config_reloader->reload();
}

void UsersConfigAccessStorage::startPeriodicReloading()
{
    std::lock_guard lock{load_mutex};
    if (config_reloader)
        config_reloader->start();
}

void UsersConfigAccessStorage::stopPeriodicReloading()
{
    std::lock_guard lock{load_mutex};
    if (config_reloader)
        config_reloader->stop();
}

std::optional<UUID> UsersConfigAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    return memory_storage.find(type, name);
}


std::vector<UUID> UsersConfigAccessStorage::findAllImpl(AccessEntityType type) const
{
    return memory_storage.findAll(type);
}


bool UsersConfigAccessStorage::exists(const UUID & id) const
{
    return memory_storage.exists(id);
}


AccessEntityPtr UsersConfigAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.read(id, throw_if_not_exists);
}


std::optional<String> UsersConfigAccessStorage::readNameImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.readName(id, throw_if_not_exists);
}


scope_guard UsersConfigAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(id, handler);
}


scope_guard UsersConfigAccessStorage::subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(type, handler);
}


bool UsersConfigAccessStorage::hasSubscription(const UUID & id) const
{
    return memory_storage.hasSubscription(id);
}


bool UsersConfigAccessStorage::hasSubscription(AccessEntityType type) const
{
    return memory_storage.hasSubscription(type);
}
}
