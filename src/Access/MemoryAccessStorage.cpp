#include <Access/MemoryAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <Backups/RestorerFromBackup.h>
#include <Backups/RestoreSettings.h>
#include <base/scope_guard.h>
#include <boost/container/flat_set.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
MemoryAccessStorage::MemoryAccessStorage(const String & storage_name_, AccessChangesNotifier & changes_notifier_, bool allow_backup_)
    : IAccessStorage(storage_name_), changes_notifier(changes_notifier_), backup_allowed(allow_backup_)
{
}


std::optional<UUID> MemoryAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    Entry & entry = *(it->second);
    return entry.id;
}


std::vector<UUID> MemoryAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    result.reserve(entries_by_id.size());
    for (const auto & [id, entry] : entries_by_id)
        if (entry.entity->isTypeOf(type))
            result.emplace_back(id);
    return result;
}


bool MemoryAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.contains(id);
}


AccessEntityPtr MemoryAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
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
    const Entry & entry = it->second;
    return entry.entity;
}


std::optional<UUID> MemoryAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    UUID id = generateRandomID();
    if (insertWithID(id, new_entity, replace_if_exists, throw_if_exists))
        return id;

    return std::nullopt;
}


bool MemoryAccessStorage::insertWithID(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    std::lock_guard lock{mutex};
    return insertNoLock(id, new_entity, replace_if_exists, throw_if_exists);
}


bool MemoryAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    const String & name = new_entity->getName();
    AccessEntityType type = new_entity->getType();

    /// Check that we can insert.
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it_by_name = entries_by_name.find(name);
    bool name_collision = (it_by_name != entries_by_name.end());

    if (name_collision && !replace_if_exists)
    {
        if (throw_if_exists)
            throwNameCollisionCannotInsert(type, name);
        else
            return false;
    }

    auto it_by_id = entries_by_id.find(id);
    if (it_by_id != entries_by_id.end())
    {
        const auto & existing_entry = it_by_id->second;
        throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getName());
    }

    if (name_collision && replace_if_exists)
    {
        const auto & existing_entry = *(it_by_name->second);
        removeNoLock(existing_entry.id, /* throw_if_not_exists = */ false);
    }

    /// Do insertion.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.entity = new_entity;
    entries_by_name[name] = &entry;
    changes_notifier.onEntityAdded(id, new_entity);
    return true;
}


bool MemoryAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    std::lock_guard lock{mutex};
    return removeNoLock(id, throw_if_not_exists);
}


bool MemoryAccessStorage::removeNoLock(const UUID & id, bool throw_if_not_exists)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    Entry & entry = it->second;
    const String & name = entry.entity->getName();
    AccessEntityType type = entry.entity->getType();

    /// Do removing.
    UUID removed_id = id;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name.erase(name);
    entries_by_id.erase(it);

    changes_notifier.onEntityRemoved(removed_id, type);
    return true;
}


bool MemoryAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    std::lock_guard lock{mutex};
    return updateNoLock(id, update_func, throw_if_not_exists);
}


bool MemoryAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    Entry & entry = it->second;
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    if (*new_entity == *old_entity)
        return true;

    entry.entity = new_entity;

    if (new_entity->getName() != old_entity->getName())
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(old_entity->getType())];
        auto it2 = entries_by_name.find(new_entity->getName());
        if (it2 != entries_by_name.end())
            throwNameCollisionCannotRename(old_entity->getType(), old_entity->getName(), new_entity->getName());

        entries_by_name.erase(old_entity->getName());
        entries_by_name[new_entity->getName()] = &entry;
    }

    changes_notifier.onEntityUpdated(id, new_entity);
    return true;
}


void MemoryAccessStorage::setAll(const std::vector<AccessEntityPtr> & all_entities)
{
    std::vector<std::pair<UUID, AccessEntityPtr>> entities_with_ids;
    entities_with_ids.reserve(all_entities.size());
    for (const auto & entity : all_entities)
        entities_with_ids.emplace_back(generateRandomID(), entity);
    setAll(entities_with_ids);
}


void MemoryAccessStorage::setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities)
{
    std::lock_guard lock{mutex};

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
            if (entry.entity->getType() != entity->getType())
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

    /// Remove entities which are not used anymore and which are in conflict with new entities.
    boost::container::flat_set<UUID> ids_to_remove = std::move(not_used_ids);
    boost::range::copy(conflicting_ids, std::inserter(ids_to_remove, ids_to_remove.end()));
    for (const auto & id : ids_to_remove)
        removeNoLock(id, /* throw_if_not_exists = */ false);

    /// Insert or update entities.
    for (const auto & [id, entity] : all_entities)
    {
        auto it = entries_by_id.find(id);
        if (it != entries_by_id.end())
        {
            if (*(it->second.entity) != *entity)
            {
                const AccessEntityPtr & changed_entity = entity;
                updateNoLock(id,
                             [&changed_entity](const AccessEntityPtr &) { return changed_entity; },
                             /* throw_if_not_exists = */ true);
            }
        }
        else
        {
            insertNoLock(id, entity, /* replace_if_exists = */ false, /* throw_if_exists = */ true);
        }
    }
}


void MemoryAccessStorage::restoreFromBackup(RestorerFromBackup & restorer)
{
    if (!isRestoreAllowed())
        throwRestoreNotAllowed();

    auto entities = restorer.getAccessEntitiesToRestore();
    if (entities.empty())
        return;

    auto create_access = restorer.getRestoreSettings().create_access;
    bool replace_if_exists = (create_access == RestoreAccessCreationMode::kReplace);
    bool throw_if_exists = (create_access == RestoreAccessCreationMode::kCreate);

    restorer.addDataRestoreTask([this, entities = std::move(entities), replace_if_exists, throw_if_exists]
    {
        for (const auto & [id, entity] : entities)
            insertWithID(id, entity, replace_if_exists, throw_if_exists);
    });
}

}
