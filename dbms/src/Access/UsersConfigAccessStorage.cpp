#include <Access/UsersConfigAccessStorage.h>
#include <Access/Quota.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/MD5Engine.h>
#include <cstring>


namespace DB
{
namespace
{
    char getTypeChar(std::type_index type)
    {
        if (type == typeid(Quota))
            return 'Q';
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
}


UsersConfigAccessStorage::UsersConfigAccessStorage() : IAccessStorage("users.xml")
{
}


UsersConfigAccessStorage::~UsersConfigAccessStorage() {}


void UsersConfigAccessStorage::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    for (const auto & entity : parseQuotas(config, getLogger()))
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


IAccessStorage::SubscriptionPtr UsersConfigAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(id, handler);
}


IAccessStorage::SubscriptionPtr UsersConfigAccessStorage::subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const
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
