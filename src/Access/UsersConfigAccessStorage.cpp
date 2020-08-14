#include <Access/UsersConfigAccessStorage.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <Access/SettingsProfile.h>
#include <Dictionaries/IDictionary.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/MD5Engine.h>
#include <common/logger_useful.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/map.hpp>
#include <cstring>


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
    char getTypeChar(std::type_index type)
    {
        if (type == typeid(User))
            return 'U';
        if (type == typeid(Quota))
            return 'Q';
        if (type == typeid(RowPolicy))
            return 'P';
        if (type == typeid(SettingsProfile))
            return 'S';
        return 0;
    }


    UUID generateID(std::type_index type, const String & name)
    {
        Poco::MD5Engine md5;
        md5.update(name);
        char type_storage_chars[] = " USRSXML";
        type_storage_chars[0] = getTypeChar(type);
        md5.update(type_storage_chars, strlen(type_storage_chars));
        UUID result;
        memcpy(&result, md5.digest().data(), md5.digestLength());
        return result;
    }


    UUID generateID(const IAccessEntity & entity) { return generateID(entity.getType(), entity.getFullName()); }

    UserPtr parseUser(const Poco::Util::AbstractConfiguration & config, const String & user_name)
    {
        auto user = std::make_shared<User>();
        user->setName(user_name);

        String user_config = "users." + user_name;

        bool has_no_password = config.has(user_config + ".no_password");
        bool has_password_plaintext = config.has(user_config + ".password");
        bool has_password_sha256_hex = config.has(user_config + ".password_sha256_hex");
        bool has_password_double_sha1_hex = config.has(user_config + ".password_double_sha1_hex");

        size_t num_password_fields = has_no_password + has_password_plaintext + has_password_sha256_hex + has_password_double_sha1_hex;
        if (num_password_fields > 1)
            throw Exception("More than one field of 'password', 'password_sha256_hex', 'password_double_sha1_hex', 'no_password' are used to specify password for user " + user_name + ". Must be only one of them.",
                ErrorCodes::BAD_ARGUMENTS);

        if (num_password_fields < 1)
            throw Exception("Either 'password' or 'password_sha256_hex' or 'password_double_sha1_hex' or 'no_password' must be specified for user " + user_name + ".", ErrorCodes::BAD_ARGUMENTS);

        if (has_password_plaintext)
        {
            user->authentication = Authentication{Authentication::PLAINTEXT_PASSWORD};
            user->authentication.setPassword(config.getString(user_config + ".password"));
        }
        else if (has_password_sha256_hex)
        {
            user->authentication = Authentication{Authentication::SHA256_PASSWORD};
            user->authentication.setPasswordHashHex(config.getString(user_config + ".password_sha256_hex"));
        }
        else if (has_password_double_sha1_hex)
        {
            user->authentication = Authentication{Authentication::DOUBLE_SHA1_PASSWORD};
            user->authentication.setPasswordHashHex(config.getString(user_config + ".password_double_sha1_hex"));
        }

        const auto profile_name_config = user_config + ".profile";
        if (config.has(profile_name_config))
        {
            auto profile_name = config.getString(profile_name_config);
            SettingsProfileElement profile_element;
            profile_element.parent_profile = generateID(typeid(SettingsProfile), profile_name);
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

        user->access.grant(AccessType::ALL); /// By default all databases are accessible.

        if (databases)
        {
            user->access.revoke(AccessFlags::allFlags() - AccessFlags::allGlobalFlags());
            user->access.grant(AccessFlags::allDictionaryFlags(), IDictionary::NO_DATABASE_TAG);
            for (const String & database : *databases)
                user->access.grant(AccessFlags::allFlags(), database);
        }

        if (dictionaries)
        {
            user->access.revoke(AccessFlags::allDictionaryFlags(), IDictionary::NO_DATABASE_TAG);
            for (const String & dictionary : *dictionaries)
                user->access.grant(AccessFlags::allDictionaryFlags(), IDictionary::NO_DATABASE_TAG, dictionary);
        }

        user->access_with_grant_option = user->access; /// By default the user can grant everything he has.

        bool access_management = config.getBool(user_config + ".access_management", false);
        if (!access_management)
        {
            user->access.revoke(AccessType::ACCESS_MANAGEMENT);
            user->access_with_grant_option.clear();
        }

        return user;
    }


    std::vector<AccessEntityPtr> parseUsers(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        Poco::Util::AbstractConfiguration::Keys user_names;
        config.keys("users", user_names);

        std::vector<AccessEntityPtr> users;
        users.reserve(user_names.size());
        for (const auto & user_name : user_names)
        {
            try
            {
                users.push_back(parseUser(config, user_name));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Could not parse user " + backQuote(user_name));
            }
        }
        return users;
    }


    QuotaPtr parseQuota(const Poco::Util::AbstractConfiguration & config, const String & quota_name, const std::vector<UUID> & user_ids)
    {
        auto quota = std::make_shared<Quota>();
        quota->setName(quota_name);

        using KeyType = Quota::KeyType;
        String quota_config = "quotas." + quota_name;
        if (config.has(quota_config + ".keyed_by_ip"))
            quota->key_type = KeyType::IP_ADDRESS;
        else if (config.has(quota_config + ".keyed"))
            quota->key_type = KeyType::CLIENT_KEY_OR_USER_NAME;
        else
            quota->key_type = KeyType::USER_NAME;

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

            using ResourceType = Quota::ResourceType;
            limits.max[ResourceType::QUERIES] = config.getUInt64(interval_config + ".queries", Quota::UNLIMITED);
            limits.max[ResourceType::ERRORS] = config.getUInt64(interval_config + ".errors", Quota::UNLIMITED);
            limits.max[ResourceType::RESULT_ROWS] = config.getUInt64(interval_config + ".result_rows", Quota::UNLIMITED);
            limits.max[ResourceType::RESULT_BYTES] = config.getUInt64(interval_config + ".result_bytes", Quota::UNLIMITED);
            limits.max[ResourceType::READ_ROWS] = config.getUInt64(interval_config + ".read_rows", Quota::UNLIMITED);
            limits.max[ResourceType::READ_BYTES] = config.getUInt64(interval_config + ".read_bytes", Quota::UNLIMITED);
            limits.max[ResourceType::EXECUTION_TIME] = Quota::secondsToExecutionTime(config.getUInt64(interval_config + ".execution_time", Quota::UNLIMITED));
        }

        quota->to_roles.add(user_ids);

        return quota;
    }


    std::vector<AccessEntityPtr> parseQuotas(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        Poco::Util::AbstractConfiguration::Keys user_names;
        config.keys("users", user_names);
        std::unordered_map<String, std::vector<UUID>> quota_to_user_ids;
        for (const auto & user_name : user_names)
        {
            if (config.has("users." + user_name + ".quota"))
                quota_to_user_ids[config.getString("users." + user_name + ".quota")].push_back(generateID(typeid(User), user_name));
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
            catch (...)
            {
                tryLogCurrentException(log, "Could not parse quota " + backQuote(quota_name));
            }
        }
        return quotas;
    }


    std::vector<AccessEntityPtr> parseRowPolicies(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        std::map<std::pair<String /* database */, String /* table */>, std::unordered_map<String /* user */, String /* filter */>> all_filters_map;
        Poco::Util::AbstractConfiguration::Keys user_names;

        try
        {
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
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse row policies");
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
                policy->setFullName(database, table_name, user_name);
                policy->conditions[RowPolicy::SELECT_FILTER] = filter;
                policy->to_roles.add(generateID(typeid(User), user_name));
                policies.push_back(policy);
            }
        }
        return policies;
    }


    SettingsProfileElements parseSettingsConstraints(const Poco::Util::AbstractConfiguration & config,
                                                     const String & path_to_constraints)
    {
        SettingsProfileElements profile_elements;
        Poco::Util::AbstractConfiguration::Keys names;
        config.keys(path_to_constraints, names);
        for (const String & name : names)
        {
            SettingsProfileElement profile_element;
            profile_element.name = name;
            Poco::Util::AbstractConfiguration::Keys constraint_types;
            String path_to_name = path_to_constraints + "." + name;
            config.keys(path_to_name, constraint_types);
            for (const String & constraint_type : constraint_types)
            {
                if (constraint_type == "min")
                    profile_element.min_value = Settings::valueToCorrespondingType(name, config.getString(path_to_name + "." + constraint_type));
                else if (constraint_type == "max")
                    profile_element.max_value = Settings::valueToCorrespondingType(name, config.getString(path_to_name + "." + constraint_type));
                else if (constraint_type == "readonly")
                    profile_element.readonly = true;
                else
                    throw Exception("Setting " + constraint_type + " value for " + name + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
            }
            profile_elements.push_back(std::move(profile_element));
        }
        return profile_elements;
    }

    std::shared_ptr<SettingsProfile> parseSettingsProfile(
        const Poco::Util::AbstractConfiguration & config,
        const String & profile_name)
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
                profile_element.parent_profile = generateID(typeid(SettingsProfile), parent_profile_name);
                profile->elements.emplace_back(std::move(profile_element));
                continue;
            }

            if (key == "constraints" || key.starts_with("constraints["))
            {
                profile->elements.merge(parseSettingsConstraints(config, profile_config + "." + key));
                continue;
            }

            SettingsProfileElement profile_element;
            profile_element.name = key;
            profile_element.value = Settings::valueToCorrespondingType(key, config.getString(profile_config + "." + key));
            profile->elements.emplace_back(std::move(profile_element));
        }

        return profile;
    }


    std::vector<AccessEntityPtr> parseSettingsProfiles(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        std::vector<AccessEntityPtr> profiles;
        Poco::Util::AbstractConfiguration::Keys profile_names;
        config.keys("profiles", profile_names);
        for (const auto & profile_name : profile_names)
        {
            try
            {
                profiles.push_back(parseSettingsProfile(config, profile_name));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Could not parse profile " + backQuote(profile_name));
            }
        }
        return profiles;
    }
}


UsersConfigAccessStorage::UsersConfigAccessStorage() : IAccessStorage("users.xml")
{
}


void UsersConfigAccessStorage::setConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    for (const auto & entity : parseUsers(config, getLogger()))
        all_entities.emplace_back(generateID(*entity), entity);
    for (const auto & entity : parseQuotas(config, getLogger()))
        all_entities.emplace_back(generateID(*entity), entity);
    for (const auto & entity : parseRowPolicies(config, getLogger()))
        all_entities.emplace_back(generateID(*entity), entity);
    for (const auto & entity : parseSettingsProfiles(config, getLogger()))
        all_entities.emplace_back(generateID(*entity), entity);
    memory_storage.setAll(all_entities);
}


std::optional<UUID> UsersConfigAccessStorage::findImpl(std::type_index type, const String & name) const
{
    return memory_storage.find(type, name);
}


std::vector<UUID> UsersConfigAccessStorage::findAllImpl(std::type_index type) const
{
    return memory_storage.findAll(type);
}


bool UsersConfigAccessStorage::existsImpl(const UUID & id) const
{
    return memory_storage.exists(id);
}


AccessEntityPtr UsersConfigAccessStorage::readImpl(const UUID & id) const
{
    return memory_storage.read(id);
}


String UsersConfigAccessStorage::readNameImpl(const UUID & id) const
{
    return memory_storage.readName(id);
}


UUID UsersConfigAccessStorage::insertImpl(const AccessEntityPtr & entity, bool)
{
    throwReadonlyCannotInsert(entity->getType(), entity->getFullName());
}


void UsersConfigAccessStorage::removeImpl(const UUID & id)
{
    auto entity = read(id);
    throwReadonlyCannotRemove(entity->getType(), entity->getFullName());
}


void UsersConfigAccessStorage::updateImpl(const UUID & id, const UpdateFunc &)
{
    auto entity = read(id);
    throwReadonlyCannotUpdate(entity->getType(), entity->getFullName());
}


ext::scope_guard UsersConfigAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(id, handler);
}


ext::scope_guard UsersConfigAccessStorage::subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(type, handler);
}


bool UsersConfigAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    return memory_storage.hasSubscription(id);
}


bool UsersConfigAccessStorage::hasSubscriptionImpl(std::type_index type) const
{
    return memory_storage.hasSubscription(type);
}
}
