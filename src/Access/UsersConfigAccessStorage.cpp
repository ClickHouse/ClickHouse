#include <Access/UsersConfigAccessStorage.h>
#include <Access/UsersConfigParser.h>
#include <Access/AccessControl.h>
#include <Access/AccessChangesNotifier.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <cstring>
#include <filesystem>


namespace DB
{
UsersConfigAccessStorage::UsersConfigAccessStorage(const String & storage_name_, AccessControl & access_control_, bool allow_backup_)
    : IAccessStorage(storage_name_)
    , access_control(access_control_)
    , memory_storage(storage_name_, access_control.getChangesNotifier(), false)
    , backup_allowed(allow_backup_)
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
        UsersConfigParser parser{access_control};
        auto allowed_profile_ids = UsersConfigParser::getAllowedIDs(config, "profiles", AccessEntityType::SETTINGS_PROFILE);
        auto role_ids_from_users_config = UsersConfigParser::getAllowedIDs(config, "roles", AccessEntityType::ROLE);

        std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
        std::unordered_map<UUID, UUID> aliases;

        auto add = [&](const AccessEntityPtr & entity)
        {
            auto canonical = UsersConfigParser::generateID(*entity);
            all_entities.emplace_back(canonical, entity);

            /// Only entities whose UUID can be referenced from another storage need a legacy alias, and only
            /// these have a plain name that is a single escaped config key (a row policy name is composite, so
            /// re-escaping its dots would not reproduce a config key).
            auto type = entity->getType();
            if (type != AccessEntityType::USER && type != AccessEntityType::ROLE
                && type != AccessEntityType::QUOTA && type != AccessEntityType::SETTINGS_PROFILE)
                return;

            const auto & name = entity->getName();
            if (name.find('.') == String::npos)
                return;
            /// unescapeDots only ever undoes '.' -> '\.', so re-escaping every dot reproduces the original
            /// config key an older server generated the entity UUID from. Alias that UUID to the canonical one
            /// so references persisted in other storages (disk/replicated `GRANT ID(...)`) still resolve.
            auto legacy = UsersConfigParser::generateID(type, Poco::replace(name, ".", "\\."));
            if (legacy != canonical)
                aliases.emplace(legacy, canonical);
        };

        for (const auto & entity : parser.parseUsers(config, allowed_profile_ids, role_ids_from_users_config))
            add(entity);

        for (const auto & entity : parser.parseQuotas(config))
            add(entity);

        for (const auto & entity : parser.parseRowPolicies(config))
            add(entity);

        for (const auto & entity : parser.parseSettingsProfiles(config, allowed_profile_ids))
            add(entity);

        for (const auto & entity : parser.parseRoles(config, role_ids_from_users_config))
            add(entity);

        memory_storage.setAll(all_entities);
        {
            std::lock_guard lock{legacy_ids_mutex};
            legacy_ids = std::move(aliases);
        }
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
    auto zk_node_cache = std::make_unique<zkutil::ZooKeeperNodeCache>(get_zookeeper_function);
    config_reloader = std::make_unique<ConfigReloader>(
        users_config_path,
        std::vector{{include_from_path}},
        preprocessed_dir,
        std::move(zk_node_cache),
        std::make_shared<Poco::Event>(),
        [&](Poco::AutoPtr<Poco::Util::AbstractConfiguration> new_config, bool /*initial_loading*/)
        {
            Settings::checkNoSettingNamesAtTopLevel(*new_config, users_config_path);
            parseFromConfig(*new_config);
            access_control.getChangesNotifier().sendNotifications();
        });
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

void UsersConfigAccessStorage::reload(ReloadMode /* reload_mode */)
{
    std::lock_guard lock{load_mutex};
    if (config_reloader)
        config_reloader->reload();
}

std::optional<UUID> UsersConfigAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    return memory_storage.find(type, name);
}


std::vector<UUID> UsersConfigAccessStorage::findAllImpl(AccessEntityType type) const
{
    return memory_storage.findAll(type);
}


UUID UsersConfigAccessStorage::resolveLegacyID(const UUID & id) const
{
    std::lock_guard lock{legacy_ids_mutex};
    auto it = legacy_ids.find(id);
    return it == legacy_ids.end() ? id : it->second;
}


bool UsersConfigAccessStorage::exists(const UUID & id) const
{
    return memory_storage.exists(id) || memory_storage.exists(resolveLegacyID(id));
}


AccessEntityPtr UsersConfigAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.read(resolveLegacyID(id), throw_if_not_exists);
}


std::optional<std::pair<String, AccessEntityType>> UsersConfigAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.readNameWithType(resolveLegacyID(id), throw_if_not_exists);
}

}
