#include <Access/MemoryAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <base/scope_guard.h>
#include <boost/container/flat_set.hpp>
#include <boost/range/adaptor/map.hpp>


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


bool MemoryAccessStorage::insertImpl(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    std::lock_guard lock{mutex};
    return insertNoLock(id, new_entity, replace_if_exists, throw_if_exists, conflicting_id);
}


bool MemoryAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    const String & name = new_entity->getName();
    AccessEntityType type = new_entity->getType();

    /// Check that we can insert.
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it_by_name = entries_by_name.find(name);
    bool name_collision = (it_by_name != entries_by_name.end());
    UUID id_by_name;
    if (name_collision)
        id_by_name = it_by_name->second->id;

    if (name_collision && !replace_if_exists)
    {
        if (throw_if_exists)
        {
            throwNameCollisionCannotInsert(type, name);
        }
        else
        {
            if (conflicting_id)
                *conflicting_id = id_by_name;
            return false;
        }
    }

    auto it_by_id = entries_by_id.find(id);
    bool id_collision = (it_by_id != entries_by_id.end());
    if (id_collision && !replace_if_exists)
    {
        const auto & existing_entry = it_by_id->second;
        if (throw_if_exists)
        {
            throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getName());
        }
        else
        {
            if (conflicting_id)
                *conflicting_id = id;
            return false;
        }
    }

    /// Remove collisions if necessary.
    if (name_collision && (id_by_name != id))
    {
        assert(replace_if_exists);
        removeNoLock(id_by_name, /* throw_if_not_exists= */ true); // NOLINT
    }

    if (id_collision)
    {
        assert(replace_if_exists);
        auto & existing_entry = it_by_id->second;
        if (existing_entry.entity->getType() == new_entity->getType())
        {
            if (*existing_entry.entity != *new_entity)
            {
                if (existing_entry.entity->getName() != new_entity->getName())
                {
                    entries_by_name.erase(existing_entry.entity->getName());
                    [[maybe_unused]] bool inserted = entries_by_name.emplace(new_entity->getName(), &existing_entry).second;
                    assert(inserted);
                }
                existing_entry.entity = new_entity;
                changes_notifier.onEntityUpdated(id, new_entity);
            }
            return true;
        }
        removeNoLock(id, /* throw_if_not_exists= */ true); // NOLINT
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


void MemoryAccessStorage::removeAllExcept(const std::vector<UUID> & ids_to_keep)
{
    std::lock_guard lock{mutex};
    removeAllExceptNoLock(ids_to_keep);
}

void MemoryAccessStorage::removeAllExceptNoLock(const std::vector<UUID> & ids_to_keep)
{
    removeAllExceptNoLock(boost::container::flat_set<UUID>{ids_to_keep.begin(), ids_to_keep.end()});
}

void MemoryAccessStorage::removeAllExceptNoLock(const boost::container::flat_set<UUID> & ids_to_keep)
{
    for (auto it = entries_by_id.begin(); it != entries_by_id.end();)
    {
        const auto & id = it->first;
        ++it; /// We must go to the next element in the map `entries_by_id` here because otherwise removeNoLock() can invalidate our iterator.
        if (!ids_to_keep.contains(id))
            removeNoLock(id, /* throw_if_not_exists */ true); // NOLINT
    }
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

    /// Remove conflicting entities from the specified list.
    auto entities_without_conflicts = all_entities;
    clearConflictsInEntitiesList(entities_without_conflicts, getLogger());

    /// Remove entities which are not used anymore.
    boost::container::flat_set<UUID> ids_to_keep;
    ids_to_keep.reserve(entities_without_conflicts.size());
    for (const auto & [id, _] : entities_without_conflicts)
        ids_to_keep.insert(id);
    removeAllExceptNoLock(ids_to_keep);

    /// Insert or update entities.
    for (const auto & [id, entity] : entities_without_conflicts)
        insertNoLock(id, entity, /* replace_if_exists = */ true, /* throw_if_exists = */ false, /* conflicting_id = */ nullptr);
}

}
