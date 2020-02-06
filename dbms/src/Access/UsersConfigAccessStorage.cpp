#include <Access/UsersConfigAccessStorage.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <Dictionaries/IDictionary.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/MD5Engine.h>
#include <cstring>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
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

        bool has_password = config.has(user_config + ".password");
        bool has_password_sha256_hex = config.has(user_config + ".password_sha256_hex");
        bool has_password_double_sha1_hex = config.has(user_config + ".password_double_sha1_hex");

        if (has_password + has_password_sha256_hex + has_password_double_sha1_hex > 1)
            throw Exception("More than one field of 'password', 'password_sha256_hex', 'password_double_sha1_hex' is used to specify password for user " + user_name + ". Must be only one of them.",
                ErrorCodes::BAD_ARGUMENTS);

        if (!has_password && !has_password_sha256_hex && !has_password_double_sha1_hex)
            throw Exception("Either 'password' or 'password_sha256_hex' or 'password_double_sha1_hex' must be specified for user " + user_name + ".", ErrorCodes::BAD_ARGUMENTS);

        if (has_password)
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

        user->profile = config.getString(user_config + ".profile");

        /// Fill list of allowed hosts.
        const auto networks_config = user_config + ".networks";
        if (config.has(networks_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(networks_config, keys);
            for (const String & key : keys)
            {
                String value = config.getString(networks_config + "." + key);
                if (key.starts_with("ip"))
                    user->allowed_client_hosts.addSubnet(value);
                else if (key.starts_with("host_regexp"))
                    user->allowed_client_hosts.addHostRegexp(value);
                else if (key.starts_with("host"))
                    user->allowed_client_hosts.addHostName(value);
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
            user->access.fullRevoke(AccessFlags::databaseLevel());
            for (const String & database : *databases)
                user->access.grant(AccessFlags::databaseLevel(), database);
            user->access.grant(AccessFlags::databaseLevel(), "system"); /// Anyone has access to the "system" database.
        }

        if (dictionaries)
        {
            user->access.fullRevoke(AccessType::dictGet, IDictionary::NO_DATABASE_TAG);
            for (const String & dictionary : *dictionaries)
                user->access.grant(AccessType::dictGet, IDictionary::NO_DATABASE_TAG, dictionary);
        }
        else if (databases)
            user->access.grant(AccessType::dictGet, IDictionary::NO_DATABASE_TAG);

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


    QuotaPtr parseQuota(const Poco::Util::AbstractConfiguration & config, const String & quota_name, const Strings & user_names)
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

        quota->roles = user_names;

        return quota;
    }


    std::vector<AccessEntityPtr> parseQuotas(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        Poco::Util::AbstractConfiguration::Keys user_names;
        config.keys("users", user_names);
        std::unordered_map<String, Strings> quota_to_user_names;
        for (const auto & user_name : user_names)
        {
            if (config.has("users." + user_name + ".quota"))
                quota_to_user_names[config.getString("users." + user_name + ".quota")].push_back(user_name);
        }

        Poco::Util::AbstractConfiguration::Keys quota_names;
        config.keys("quotas", quota_names);
        std::vector<AccessEntityPtr> quotas;
        quotas.reserve(quota_names.size());
        for (const auto & quota_name : quota_names)
        {
            try
            {
                auto it = quota_to_user_names.find(quota_name);
                const Strings quota_users = (it != quota_to_user_names.end()) ? std::move(it->second) : Strings{};
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
        std::vector<AccessEntityPtr> policies;
        Poco::Util::AbstractConfiguration::Keys user_names;
        config.keys("users", user_names);

        for (const String & user_name : user_names)
        {
            const String databases_config = "users." + user_name + ".databases";
            if (config.has(databases_config))
            {
                Poco::Util::AbstractConfiguration::Keys databases;
                config.keys(databases_config, databases);

                /// Read tables within databases
                for (const String & database : databases)
                {
                    const String database_config = databases_config + "." + database;
                    Poco::Util::AbstractConfiguration::Keys keys_in_database_config;
                    config.keys(database_config, keys_in_database_config);

                    /// Read table properties
                    for (const String & key_in_database_config : keys_in_database_config)
                    {
                        String table_name = key_in_database_config;
                        String filter_config = database_config + "." + table_name + ".filter";

                        if (key_in_database_config.starts_with("table["))
                        {
                            const auto table_name_config = database_config + "." + table_name + "[@name]";
                            if (config.has(table_name_config))
                            {
                                table_name = config.getString(table_name_config);
                                filter_config = database_config + ".table[@name='" + table_name + "']";
                            }
                        }

                        if (config.has(filter_config))
                        {
                            try
                            {
                                auto policy = std::make_shared<RowPolicy>();
                                policy->setFullName(database, table_name, user_name);
                                policy->conditions[RowPolicy::SELECT_FILTER] = config.getString(filter_config);
                                policy->roles.push_back(user_name);
                                policies.push_back(policy);
                            }
                            catch (...)
                            {
                                tryLogCurrentException(
                                    log,
                                    "Could not parse row policy " + backQuote(user_name) + " on table " + backQuoteIfNeed(database) + "."
                                        + backQuoteIfNeed(table_name));
                            }
                        }
                    }
                }
            }
        }
        return policies;
    }
}


UsersConfigAccessStorage::UsersConfigAccessStorage() : IAccessStorage("users.xml")
{
}


UsersConfigAccessStorage::~UsersConfigAccessStorage() {}


void UsersConfigAccessStorage::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    for (const auto & entity : parseUsers(config, getLogger()))
        all_entities.emplace_back(generateID(*entity), entity);
    for (const auto & entity : parseQuotas(config, getLogger()))
        all_entities.emplace_back(generateID(*entity), entity);
    for (const auto & entity : parseRowPolicies(config, getLogger()))
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
