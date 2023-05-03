#include "ConfigFDBAccessStorage.h"
#include <Access/AccessEntityConvertor.h>
#include <Access/IAccessEntity.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Core/Settings.h>
#include <Dictionaries/IDictionary.h>
#include <base/FnTraits.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <Poco/MD5Engine.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{


ConfigFDBAccessStorage::ConfigFDBAccessStorage(
    const String & storage_name_,
    const String & config_path_,
    const Poco::Util::AbstractConfiguration & config_,
    const std::function<FoundationDBPtr()> & get_fdb_function_,
    const CheckSettingNameFunction & check_setting_name_function_,
    const IsNoPasswordFunction & is_no_password_allowed_function_,
    const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_)
    : FDBAccessStorage(storage_name_, get_fdb_function_())
    , check_setting_name_function(check_setting_name_function_)
    , is_no_password_allowed_function(is_no_password_allowed_function_)
    , is_plaintext_password_allowed_function(is_plaintext_password_allowed_function_)
    , config_path(config_path_)
    , config(const_cast<Poco::Util::AbstractConfiguration &>(config_))
{
    readonly = true;
    scope = AccessEntityScope::CONFIG;
    if (meta_store->isFirstBoot())
    {
        /// Node Startups firstly. Need to migrate Metadata from local Configurations files into Foundationdb => Will call setAll function.
        is_first_startup = true;
        tryClearEntitiesOnFDB();
        setConfig();
    }
    else
    {
        /// Node doesn't belong to startup firstly. Need to pull Metadata (All access entities) from FoundationDB into Memory.
        is_first_startup = false;
        auto all_entities = pullAllEntitiesFromFDB();
        setAll(all_entities);
    }
}

ConfigFDBAccessStorage::ConfigFDBAccessStorage(
    const String & config_path_,
    const Poco::Util::AbstractConfiguration & config_,
    const std::function<FoundationDBPtr()> & get_fdb_function_,
    const CheckSettingNameFunction & check_setting_name_function_,
    const IsNoPasswordFunction & is_no_password_allowed_function_,
    const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_)
    : ConfigFDBAccessStorage(
        STORAGE_TYPE,
        config_path_,
        config_,
        get_fdb_function_,
        check_setting_name_function_,
        is_no_password_allowed_function_,
        is_plaintext_password_allowed_function_)
{
}

/// Sets all entities at once when starts up no matter what is first-time or not.
void ConfigFDBAccessStorage::setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    setAllNoLock(all_entities, notifications);
}

void ConfigFDBAccessStorage::setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities, Notifications & notifications)
{
    /// To support future modifiable features, There will be considered the cases of having conflicts or unused access entities.
    auto [not_used_ids, conflicting_ids] = getUnusedAndConflictingEntityComparedWithMemory(all_entities);

    /// Remove entities which are not used anymore and which are in conflict with new entities.
    boost::container::flat_set<UUID> ids_to_remove = std::move(not_used_ids);
    boost::range::copy(conflicting_ids, std::inserter(ids_to_remove, ids_to_remove.end()));

    /// According to the node is started up firstly or not, need to serialize to fdb in the first time.
    /// Remove not-used and conflicted entities.
    for (const auto & id : ids_to_remove)
        if (is_first_startup)
            removeNoLock(id, /* throw_if_not_exists = */ false, notifications, [this](const UUID & id_, const AccessEntityType & type_) {
                tryDeleteEntityOnFDB(id_, type_);
            });
        else
            removeNoLock(id, /* throw_if_not_exists = */ false, notifications);

    /// Insert or update entities.
    for (const auto & [id, entity] : all_entities)
    {
        auto it = entries_by_id.find(id);
        if (it != entries_by_id.end())
        {
            if (*(it->second.entity) != *entity)
            {
                const AccessEntityPtr & changed_entity = entity;
                if (is_first_startup)
                    updateNoLock(
                        id,
                        [&changed_entity](const AccessEntityPtr &) { return changed_entity; },
                        /* throw_if_not_exists = */ true,
                        notifications,
                        [this](const UUID & id_, const IAccessEntity & entity_) { tryUpdateEntityOnFDB(id_, entity_); });
                else
                    updateNoLock(
                        id,
                        [&changed_entity](const AccessEntityPtr &) { return changed_entity; },
                        /* throw_if_not_exists = */ true,
                        notifications);
            }
        }
        else
        {
            if (is_first_startup)
                insertNoLock(
                    id,
                    entity,
                    /* replace_if_exists = */ false,
                    /* throw_if_exists = */ true,
                    notifications,
                    [this](const UUID & id_, const IAccessEntity & entity_) { tryInsertEntityToFDB(id_, entity_); });
            else
                insertNoLock(id, entity, /* replace_if_exists = */ false, /* throw_if_exists = */ true, notifications);
        }
    }
}

std::pair<boost::container::flat_set<UUID>, std::vector<UUID>>
ConfigFDBAccessStorage::getUnusedAndConflictingEntityComparedWithMemory(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities)
{
    boost::container::flat_set<UUID> not_used_ids;
    std::vector<UUID> conflicting_ids;

    /// Get the list of currently used IDs. Later we will remove those of them which are not used anymore.
    for (const auto & id : entries_by_id | boost::adaptors::map_keys)
        not_used_ids.emplace(id);

    /// Get the list of conflicting IDs and update the list of currently used ones.
    for (const auto & [id, entity] : all_entities)
    {
        auto it = entries_by_id.find(id);
        if (it != entries_by_id.end())
        {
            not_used_ids.erase(id); /// ID is used.

            Entry & entry = it->second;
            if (entry.type != entity->getType())
                conflicting_ids.emplace_back(id); /// Conflict: same ID, different type.
        }

        const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(entity->getType())];
        auto it2 = entries_by_name.find(entity->getName());
        if (it2 != entries_by_name.end())
        {
            Entry & entry = *(it2->second);
            if (entry.id != id)
                conflicting_ids.emplace_back(entry.id); /// Conflict: same name and type, different ID.
        }
    }

    return std::make_pair(not_used_ids, conflicting_ids);
}


std::vector<std::pair<UUID, AccessEntityPtr>> ConfigFDBAccessStorage::pullAllEntitiesFromFDB() const
{
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    try
    {
        const auto entities_in_fdb = meta_store->getAllAccessEntities(scope);
        all_entities.reserve(entities_in_fdb.size());

        for (const auto & [uuid, proto_value] : entities_in_fdb)
        {
            AccessEntityPtr entity = FoundationDB::fromProto(*proto_value);
            all_entities.emplace_back(std::make_pair(uuid, entity));
        }
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while pull all access entities from FoundationDB happens error!"));
        throw;
    }
    return all_entities;
}

AccessEntityPtr ConfigFDBAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return nullptr;
    }


    const auto & entry = it->second;
    return entry.entity;
}

std::vector<std::pair<UUID, AccessEntityPtr>>
ConfigFDBAccessStorage::parseEntitiesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    bool no_password_allowed = is_no_password_allowed_function();
    bool plaintext_password_allowed = is_plaintext_password_allowed_function();
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    for (const auto & entity : UsersConfigAccessStorage::parseUsers(config, no_password_allowed, plaintext_password_allowed))
        all_entities.emplace_back(UsersConfigAccessStorage::generateID(*entity), entity);
    for (const auto & entity : UsersConfigAccessStorage::parseQuotas(config))
        all_entities.emplace_back(UsersConfigAccessStorage::generateID(*entity), entity);
    for (const auto & entity : UsersConfigAccessStorage::parseRowPolicies(config))
        all_entities.emplace_back(UsersConfigAccessStorage::generateID(*entity), entity);
    for (const auto & entity : UsersConfigAccessStorage::parseSettingsProfiles(config, check_setting_name_function))
        all_entities.emplace_back(UsersConfigAccessStorage::generateID(*entity), entity);

    return all_entities;
}

void ConfigFDBAccessStorage::setConfig()
{
    try
    {
        auto all_entities = parseEntitiesFromConfig(config);
        setAll(all_entities);
    }
    catch (Exception & e)
    {
        e.addMessage(
            fmt::format("while loading {}", config_path.empty() ? "configuration" : ("configuration file " + quoteString(config_path))));
        throw;
    }
}

bool ConfigFDBAccessStorage::isPathEqual(const String & path_) const
{
    return getPath() == path_;
}

String ConfigFDBAccessStorage::getPath() const
{
    std::lock_guard lock{mutex};
    return config_path;
}
}
