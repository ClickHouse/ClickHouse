#include <Access/UsersConfigAccessStorage.h>
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
#include <Common/SSHWrapper.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
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
#include <filesystem>
#include <base/FnTraits.h>
#include <base/range.h>


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

        for (const auto & entity : parser.parseUsers(config, allowed_profile_ids, role_ids_from_users_config))
            all_entities.emplace_back(UsersConfigParser::generateID(*entity), entity);

        for (const auto & entity : parser.parseQuotas(config))
            all_entities.emplace_back(UsersConfigParser::generateID(*entity), entity);

        for (const auto & entity : parser.parseRowPolicies(config))
            all_entities.emplace_back(UsersConfigParser::generateID(*entity), entity);

        for (const auto & entity : parser.parseSettingsProfiles(config, allowed_profile_ids))
            all_entities.emplace_back(UsersConfigParser::generateID(*entity), entity);

        for (const auto & entity : parser.parseRoles(config, role_ids_from_users_config))
            all_entities.emplace_back(UsersConfigParser::generateID(*entity), entity);

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


bool UsersConfigAccessStorage::exists(const UUID & id) const
{
    return memory_storage.exists(id);
}


AccessEntityPtr UsersConfigAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.read(id, throw_if_not_exists);
}


std::optional<std::pair<String, AccessEntityType>> UsersConfigAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.readNameWithType(id, throw_if_not_exists);
}

}
