#include <Access/MemoryAccessStorage.h>
#include <base/scope_guard.h>
#include <boost/container/flat_set.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
MemoryAccessStorage::MemoryAccessStorage(const String & storage_name_)
    : IAccessStorage(storage_name_)
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
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    UUID id = generateRandomID();
    std::lock_guard lock{mutex};
    if (insertNoLock(id, new_entity, replace_if_exists, throw_if_exists, notifications))
        return id;

    return std::nullopt;
}


bool MemoryAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, Notifications & notifications)
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
        removeNoLock(existing_entry.id, /* throw_if_not_exists = */ false, notifications);
    }

    /// Do insertion.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.entity = new_entity;
    entries_by_name[name] = &entry;
    prepareNotifications(entry, false, notifications);
    return true;
}


bool MemoryAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    return removeNoLock(id, throw_if_not_exists, notifications);
}


bool MemoryAccessStorage::removeNoLock(const UUID & id, bool throw_if_not_exists, Notifications & notifications)
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

    prepareNotifications(entry, true, notifications);

    /// Do removing.
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name.erase(name);
    entries_by_id.erase(it);
    return true;
}


bool MemoryAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    return updateNoLock(id, update_func, throw_if_not_exists, notifications);
}


bool MemoryAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, Notifications & notifications)
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

    prepareNotifications(entry, false, notifications);
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
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    setAllNoLock(all_entities, notifications);
}


void MemoryAccessStorage::setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities, Notifications & notifications)
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
                updateNoLock(id,
                             [&changed_entity](const AccessEntityPtr &) { return changed_entity; },
                             /* throw_if_not_exists = */ true,
                             notifications);
            }
        }
        else
        {
            insertNoLock(id, entity, /* replace_if_exists = */ false, /* throw_if_exists = */ true, notifications);
        }
    }
}


void MemoryAccessStorage::prepareNotifications(const Entry & entry, bool remove, Notifications & notifications) const
{
    const AccessEntityPtr entity = remove ? nullptr : entry.entity;
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, entry.id, entity});

    for (const auto & handler : handlers_by_type[static_cast<size_t>(entry.entity->getType())])
        notifications.push_back({handler, entry.id, entity});
}


scope_guard MemoryAccessStorage::subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());

    return [this, type, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
    };
}


scope_guard MemoryAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        return {};
    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto it2 = entries_by_id.find(id);
        if (it2 != entries_by_id.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}


bool MemoryAccessStorage::hasSubscription(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it != entries_by_id.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}


bool MemoryAccessStorage::hasSubscription(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}
}
