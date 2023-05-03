#include "FDBAccessStorage.h"
#include <Access/AccessEntityConvertor.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/range/adaptor/map.hpp>
#include <Common/FoundationDB/FoundationDBCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FDB_META_EXCEPTION;
}
std::optional<UUID> FDBAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    return it->second->id;
}

std::vector<UUID> FDBAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> res;
    res.reserve(entries_by_id.size());
    for (const auto & [id, entry] : entries_by_id)
        if (entry.type == type)
            res.emplace_back(id);
    return res;
}

void FDBAccessStorage::prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const
{
    if (!remove && !entry.entity)
        return;

    const AccessEntityPtr entity = remove ? nullptr : entry.entity;
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, id, entity});

    for (const auto & handler : handlers_by_type[static_cast<size_t>(entry.type)])
        notifications.push_back({handler, id, entity});
}

scope_guard FDBAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        return {};

    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it] {
        std::lock_guard lock2{mutex};
        auto it2 = entries_by_id.find(id);
        if (it2 != entries_by_id.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}

scope_guard FDBAccessStorage::subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());

    return [this, type, handler_it] {
        std::lock_guard lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
    };
}

FDBAccessStorage::FDBAccessStorage(const String & storage_name_, std::shared_ptr<MetadataStoreFoundationDB> meta_store_)
    : IAccessStorage(storage_name_), meta_store(meta_store_)
{
    if (!meta_store)
        throw Exception("Can't initialize FoundationDBAccessStorage without FoundationDB", ErrorCodes::FDB_META_EXCEPTION);
}

bool FDBAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.count(id);
}

bool FDBAccessStorage::hasSubscription(const UUID & id) const
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

bool FDBAccessStorage::hasSubscription(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}


bool FDBAccessStorage::removeNoLock(
    const UUID & id, bool throw_if_not_exists, Notifications & notifications, const DeleteEntityInFDBFunc & remove_func)
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
    AccessEntityType & type = entry.type;

    if (remove_func != nullptr)
        remove_func(id, type);

    /// Do removing in memory
    prepareNotifications(id, entry, true, notifications);
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(entry.type)];
    entries_by_name.erase(entry.name);
    entries_by_id.erase(it);
    return true;
}


bool FDBAccessStorage::updateNoLock(
    const UUID & id,
    const UpdateFunc & update_func,
    bool throw_if_not_exists,
    Notifications & notifications,
    const WriteEntityInFDBFunc & write_func)
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
        throwBadCast(id, new_entity->getType(), new_entity->getName(), entry.type);

    if (*new_entity == *old_entity)
        return true;

    if (new_entity->getName() != old_entity->getName())
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(old_entity->getType())];
        auto it2 = entries_by_name.find(new_entity->getName());
        if (it2 != entries_by_name.end())
            throwNameCollisionCannotRename(old_entity->getType(), old_entity->getName(), new_entity->getName());

        entries_by_name.erase(old_entity->getName());
        entries_by_name[new_entity->getName()] = &entry;
    }

    if (write_func != nullptr)
        write_func(id, *new_entity);
    entry.entity = new_entity;

    prepareNotifications(id, entry, false, notifications);
    return true;
}

bool FDBAccessStorage::insertNoLock(
    const UUID & id,
    const AccessEntityPtr & new_entity,
    bool replace_if_exists,
    bool throw_if_exists,
    Notifications & notifications,
    const WriteEntityInFDBFunc & write_func)
{
    const String & name = new_entity->getName();
    AccessEntityType type = new_entity->getType();

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
        throwIDCollisionCannotInsert(id, type, name, existing_entry.type, existing_entry.name);
    }

    if (write_func != nullptr)
        write_func(id, *new_entity);

    if (name_collision && replace_if_exists)
    {
        const auto & existing_entry = *(it_by_name->second);
        removeNoLock(existing_entry.id, false, notifications);
    }

    /// Do insertion.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.entity = new_entity;
    entry.type = type;
    entry.name = name;
    entries_by_name[entry.name] = &entry;
    prepareNotifications(id, entry, false, notifications);
    return true;
}

/// Writes the proto value of a specified access entity into the entry in foundationdb.
void FDBAccessStorage::tryInsertEntityToFDB(const UUID & id, const IAccessEntity & entity) const
{
    auto proto_value = FoundationDB::toProto(entity);
    try
    {
        meta_store->addAccessEntity(scope, id, proto_value, false, true);
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while writing access entity into FoundationDB happens error!"));
        return;
    }
}

/// Reads a proto value containing ATTACH queries from foundationdb and the parses it to build an access entity.
AccessEntityPtr FDBAccessStorage::tryReadEntityFromFDB(const UUID & id) const
{
    try
    {
        auto proto_value = meta_store->getAccessEntity(scope, id, true);
        return FoundationDB::fromProto(*proto_value);
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while reading lists from FoundationDB happens error!"));
        throw;
    }
}

/// Deletes the proto value of a specified access entity on foundationdb.
void FDBAccessStorage::tryDeleteEntityOnFDB(const UUID & id, const AccessEntityType & type) const
{
    try
    {
        meta_store->removeAccessEntity(scope, id, type, true);
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while removing entity from FoundationDB happens error!"));
        throw Exception("Couldn't delete UUID_" + toString(id), ErrorCodes::FDB_META_EXCEPTION);
    }
}

/// Updates the proto value of a specified access entity on foundationdb.
/// NEED NOT TO consider others Situation, such as Collision, which is already considered at the time of calling.
void FDBAccessStorage::tryUpdateEntityOnFDB(const UUID & id, const IAccessEntity & entity) const
{
    try
    {
        auto proto_value = FoundationDB::toProto(entity);
        meta_store->updateAccessEntity(scope, id, proto_value, true);
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while updating entity from FoundationDB happens error!"));
        return;
    }
}

/// Clears all the proto values of access entities on foundationdb.
void FDBAccessStorage::tryClearEntitiesOnFDB() const
{
    try
    {
        /// Access entity and its corresponding index will be remove together.
        meta_store->clearAccessEntities(scope);
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while clear all entities from FoundationDB happens error!"));
        throw;
    }
}
}
