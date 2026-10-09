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
        std::lock_guard lock(mutex);
        auto & entry = storage_locks[table.get()];
        entry.owner = owner ? owner.get() : table.get();
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
    auto owner = resolveActionLockOwner(table);
    StorageRawPtr owner_ptr = owner ? owner.get() : table.get();

    {
        std::lock_guard lock(mutex);

        for (auto & [storage, entry] : storage_locks)
            if (storage == table.get() || entry.owner == owner_ptr)
                entry.locks.erase(action_type);
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

    for (const auto & [storage, entry] : storage_locks)
    {
        if (storage != table && entry.owner != table)
            continue;

        for (const auto & [type, action_lock] : entry.locks)
            if ((!action_type || type == *action_type) && !action_lock.expired())
                return true;
    }

    return false;
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
            it_storage = storage_locks.erase(it_storage);
        else
            ++it_storage;
    }
}

}
