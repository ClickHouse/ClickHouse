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


std::optional<UUID> MemoryAccessStorage::findImpl(EntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    Entry & entry = *(it->second);
    return entry.id;
}


std::vector<UUID> MemoryAccessStorage::findAllImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    result.reserve(entries_by_id.size());
    for (const auto & [id, entry] : entries_by_id)
        if (entry.entity->isTypeOf(type))
            result.emplace_back(id);
    return result;
}


bool MemoryAccessStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.count(id);
}


AccessEntityPtr MemoryAccessStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);
    const Entry & entry = it->second;
    return entry.entity;
}


String MemoryAccessStorage::readNameImpl(const UUID & id) const
{
    return readImpl(id)->getName();
}


UUID MemoryAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    UUID id = generateRandomID();
    std::lock_guard lock{mutex};
    insertNoLock(id, new_entity, replace_if_exists, notifications);
    return id;
}


void MemoryAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, Notifications & notifications)
{
    const String & name = new_entity->getName();
    EntityType type = new_entity->getType();

    /// Check that we can insert.
    auto it = entries_by_id.find(id);
    if (it != entries_by_id.end())
    {
        const auto & existing_entry = it->second;
        throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getName());
    }

    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it2 = entries_by_name.find(name);
    if (it2 != entries_by_name.end())
    {
        const auto & existing_entry = *(it2->second);
        if (replace_if_exists)
            removeNoLock(existing_entry.id, notifications);
        else
            throwNameCollisionCannotInsert(type, name);
    }

    /// Do insertion.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.entity = new_entity;
    entries_by_name[name] = &entry;
    prepareNotifications(entry, false, notifications);
}


void MemoryAccessStorage::removeImpl(const UUID & id)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    removeNoLock(id, notifications);
}


void MemoryAccessStorage::removeNoLock(const UUID & id, Notifications & notifications)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    Entry & entry = it->second;
    const String & name = entry.entity->getName();
    EntityType type = entry.entity->getType();

    prepareNotifications(entry, true, notifications);

    /// Do removing.
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name.erase(name);
    entries_by_id.erase(it);
}


void MemoryAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    updateNoLock(id, update_func, notifications);
}


void MemoryAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, Notifications & notifications)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    Entry & entry = it->second;
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    if (*new_entity == *old_entity)
        return;

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
        removeNoLock(id, notifications);

    /// Insert or update entities.
    for (const auto & [id, entity] : all_entities)
    {
        auto it = entries_by_id.find(id);
        if (it != entries_by_id.end())
        {
            if (*(it->second.entity) != *entity)
            {
                const AccessEntityPtr & changed_entity = entity;
                updateNoLock(id, [&changed_entity](const AccessEntityPtr &) { return changed_entity; }, notifications);
            }
        }
        else
            insertNoLock(id, entity, false, notifications);
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


scope_guard MemoryAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
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


bool MemoryAccessStorage::hasSubscriptionImpl(const UUID & id) const
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


bool MemoryAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}
}
