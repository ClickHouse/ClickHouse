#include "FDBAccessStorage.h"
#include <Access/AccessEntityConvertor.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/range/adaptor/map.hpp>
#include <utility>
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


FDBAccessStorage::FDBAccessStorage(
    const String & storage_name_, AccessChangesNotifier & changes_notifier_, std::shared_ptr<MetadataStoreFoundationDB> meta_store_)
    : IAccessStorage(storage_name_), meta_store(std::move(meta_store_)), changes_notifier(changes_notifier_)
{
    if (!meta_store)
        throw Exception(ErrorCodes::FDB_META_EXCEPTION, "Can't initialize FoundationDBAccessStorage without FoundationDB");
}

bool FDBAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.count(id);
}


bool FDBAccessStorage::removeNoLock(const UUID & id, bool throw_if_not_exists, const DeleteEntityInFDBFunc & remove_func)
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
    AccessEntityType type = entry.type;

    if (remove_func != nullptr)
        remove_func(id, type);

    /// Do removing in memory
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(entry.type)];
    entries_by_name.erase(entry.name);
    entries_by_id.erase(it);

    changes_notifier.onEntityRemoved(id, type);
    return true;
}


bool FDBAccessStorage::updateNoLock(
    const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, const WriteEntityInFDBFunc & write_func)
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

    const String & new_name = new_entity->getName();
    const String & old_name = old_entity->getName();
    const AccessEntityType type = entry.type;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

    bool name_changed = (new_name != old_name);

    if (name_changed)
    {
        if (entries_by_name.contains(new_name))
            throwNameCollisionCannotRename(type, old_name, new_name);
        if (write_func != nullptr)
            write_func(id, *new_entity);
    }

    if (write_func != nullptr)
        write_func(id, *new_entity);
    entry.entity = new_entity;

    if (name_changed)
    {
        entries_by_name.erase(entry.name);
        entry.name = new_name;
        entries_by_name[entry.name] = &entry;
    }

    changes_notifier.onEntityUpdated(id, new_entity);

    return true;
}

bool FDBAccessStorage::insertNoLock(
    const UUID & id,
    const AccessEntityPtr & new_entity,
    bool replace_if_exists,
    bool throw_if_exists,
    const WriteEntityInFDBFunc & write_func)
{
    const String & name = new_entity->getName();
    AccessEntityType type = new_entity->getType();


    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it_by_name = entries_by_name.find(name);
    bool name_collision = (it_by_name != entries_by_name.end());
    UUID id_by_name;
    if (name_collision)
        id_by_name = it_by_name->second->id;

    if (name_collision && !replace_if_exists)
    {
        if (throw_if_exists)
            throwNameCollisionCannotInsert(type, name);
        else
            return false;
    }

    auto it_by_id = entries_by_id.find(id);
    bool id_collision = (it_by_id != entries_by_id.end());
    if (id_collision && !replace_if_exists)
    {
        if (throw_if_exists)
        {
            const auto & existing_entry = it_by_id->second;
            throwIDCollisionCannotInsert(id, type, name, existing_entry.type, existing_entry.name);
        }
        else
            return false;
    }


    /// Remove collisions if necessary.
    if (name_collision && (id_by_name != id))
    {
        assert(replace_if_exists);
        removeNoLock(id_by_name, /* throw_if_not_exists= */ false);
    }

    if (id_collision)
    {
        assert(replace_if_exists);
        auto & existing_entry = it_by_id->second;
        if (existing_entry.type == new_entity->getType())
        {
            if (!existing_entry.entity || (*existing_entry.entity != *new_entity))
            {
                if (write_func != nullptr)
                    write_func(id, *new_entity);
                if (existing_entry.name != new_entity->getName())
                {
                    entries_by_name.erase(existing_entry.name);
                    [[maybe_unused]] bool inserted = entries_by_name.emplace(new_entity->getName(), &existing_entry).second;
                    assert(inserted);
                }
                existing_entry.entity = new_entity;
                changes_notifier.onEntityUpdated(id, new_entity);
            }
            return true;
        }

        removeNoLock(id, /* throw_if_not_exists= */ false);
    }

    /// Do insertion.
    if (write_func != nullptr)
        write_func(id, *new_entity);

    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.type = type;
    entry.name = name;
    entry.entity = new_entity;
    entries_by_name[entry.name] = &entry;

    changes_notifier.onEntityAdded(id, new_entity);
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
        throw Exception(ErrorCodes::FDB_META_EXCEPTION, "Couldn't delete UUID_" + toString(id));
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


