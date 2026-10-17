#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge = 1;
    extern const StorageActionBlockType PartsFetch = 2;
    extern const StorageActionBlockType PartsSend = 3;
    extern const StorageActionBlockType ReplicationQueue = 4;
    extern const StorageActionBlockType DistributedSend = 5;
    extern const StorageActionBlockType PartsTTLMerge = 6;
    extern const StorageActionBlockType PartsMove = 7;
    extern const StorageActionBlockType PullReplicationLog = 8;
    extern const StorageActionBlockType Cleanup = 9;
    extern const StorageActionBlockType ViewRefresh = 10;
    extern const StorageActionBlockType VirtualPartsUpdate = 11;
    extern const StorageActionBlockType ReduceBlockingParts = 12;
    extern const StorageActionBlockType ViewRefreshPause = 13;
    extern const StorageActionBlockType StreamConsume = 14;
}


namespace
{

/// Forwarding storages return the action locks of the storage they resolve to, so the lock
/// belongs to that storage rather than to the one it was requested from.
StoragePtr resolveActionLockOwner(StoragePtr table)
{
    /// Bound the walk in case of a cycle of aliases.
    for (size_t depth = 0; depth < 16 && table; ++depth)
    {
        StoragePtr next;
        if (const auto * alias = typeid_cast<const StorageAlias *>(table.get()))
            next = alias->tryGetTargetTable();
        else if (const auto * view = typeid_cast<const StorageMaterializedView *>(table.get()); view && view->hasInnerTable())
            next = view->tryGetTargetTable();

        if (!next)
            break;
        table = std::move(next);
    }
    return table;
}

}

ActionLocksManager::ActionLocksManager(ContextPtr context_) : WithContext(context_->getGlobalContext())
{
}

void ActionLocksManager::add(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        add(table, action_type);
}

void ActionLocksManager::add(const StoragePtr & table, StorageActionBlockType action_type)
{
    ActionLock action_lock = table->getActionLock(action_type);

    if (!action_lock.expired())
    {
        auto owner = resolveActionLockOwner(table);
        if (!owner)
            owner = table;

        std::lock_guard lock(mutex);
        auto & entry = storage_locks[table.get()];
        if (entry.owner != owner.get())
        {
            if (entry.owner)
            {
                auto it_owned = owned_storages.find(entry.owner);
                it_owned->second.erase(table.get());
                if (it_owned->second.empty())
                    owned_storages.erase(it_owned);
            }
            entry.owner = owner.get();
            owned_storages[entry.owner].insert(table.get());
        }
        entry.owner_storage = owner;
        entry.locks[action_type] = std::move(action_lock);
    }
}

void ActionLocksManager::remove(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        remove(table, action_type);
}

void ActionLocksManager::remove(const StoragePtr & table, StorageActionBlockType action_type)
{
    /// The blocker behind a lock taken through a forwarding storage belongs to the storage it resolved to,
    /// so `SYSTEM START` on either of them has to release it: remove every lock with the same owner.
    /// The owner is the one recorded when the lock was taken, so a forwarding storage is not resolved
    /// again here: its target may be unavailable, and nothing is resolved when no lock is held.
    StoragePtr owner;

    {
        std::lock_guard lock(mutex);

        StorageRawPtr owner_ptr = table.get();
        if (auto it = storage_locks.find(table.get()); it != storage_locks.end())
        {
            owner_ptr = it->second.owner;
            owner = it->second.owner_storage.lock();
            it->second.locks.erase(action_type);
        }

        if (auto it_owned = owned_storages.find(owner_ptr); it_owned != owned_storages.end())
            for (StorageRawPtr storage : it_owned->second)
                storage_locks.at(storage).locks.erase(action_type);
    }

    /// The callers notify the storage they were given; when the blocker belongs to another storage,
    /// notify it as well, so that it resumes the released action.
    if (owner && owner.get() != table.get())
        owner->onActionLockRemove(action_type);
}

bool ActionLocksManager::has(const StorageID & table_id, StorageActionBlockType action_type) const
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        return has(table, action_type);
    return false;
}

bool ActionLocksManager::has(const StoragePtr & table, StorageActionBlockType action_type) const
{
    return hasImpl(table.get(), action_type);
}

bool ActionLocksManager::hasAny(const StorageID & table_id) const
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        return hasAny(table);
    return false;
}

bool ActionLocksManager::hasAny(const StoragePtr & table) const
{
    return hasImpl(table.get(), std::nullopt);
}

bool ActionLocksManager::hasImpl(StorageRawPtr table, std::optional<StorageActionBlockType> action_type) const
{
    std::lock_guard lock(mutex);

    auto has_lock = [&](StorageRawPtr storage)
    {
        auto it = storage_locks.find(storage);
        if (it == storage_locks.end())
            return false;

        for (const auto & [type, action_lock] : it->second.locks)
            if ((!action_type || type == *action_type) && !action_lock.expired())
                return true;
        return false;
    };

    if (has_lock(table))
        return true;

    if (auto it_owned = owned_storages.find(table); it_owned != owned_storages.end())
        for (StorageRawPtr storage : it_owned->second)
            if (has_lock(storage))
                return true;

    return false;
}

void ActionLocksManager::eraseEntry(StorageLocks::iterator it)
{
    auto it_owned = owned_storages.find(it->second.owner);
    it_owned->second.erase(it->first);
    if (it_owned->second.empty())
        owned_storages.erase(it_owned);
    storage_locks.erase(it);
}

void ActionLocksManager::cleanExpired()
{
    std::lock_guard lock(mutex);

    for (auto it_storage = storage_locks.begin(); it_storage != storage_locks.end();)
    {
        auto & locks = it_storage->second.locks;
        for (auto it_lock = locks.begin(); it_lock != locks.end();)
        {
            if (it_lock->second.expired())
                it_lock = locks.erase(it_lock);
            else
                ++it_lock;
        }

        if (locks.empty())
            eraseEntry(it_storage++);
        else
            ++it_storage;
    }
}

}
