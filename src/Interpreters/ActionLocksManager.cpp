#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/StorageTableProxy.h>


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


ActionLocksManager::ActionLocksManager(ContextPtr context_) : WithContext(context_->getGlobalContext())
{
}

namespace
{

/// The key a lock for `table` is held under. A lazily loaded table is a `StorageTableProxy` until the
/// database replaces it with the loaded storage, so `SYSTEM STOP ...` and `SYSTEM START ...` may see
/// different objects for the same table. Both address the loaded storage. A proxy that has not been
/// loaded cannot hold a lock: taking one loads the table.
const IStorage * lockKey(const StoragePtr & table)
{
    if (const auto * proxy = dynamic_cast<const StorageTableProxy *>(table.get()))
        if (auto loaded = proxy->getLoadedTable())
            return loaded.get();
    return table.get();
}

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
        const auto * key = lockKey(table);
        std::lock_guard lock(mutex);
        storage_locks[key][action_type] = std::move(action_lock);
    }
}

void ActionLocksManager::remove(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        remove(table, action_type);
}

void ActionLocksManager::remove(const StoragePtr & table, StorageActionBlockType action_type)
{
    const auto * key = lockKey(table);
    std::lock_guard lock(mutex);

    if (auto it = storage_locks.find(key); it != storage_locks.end())
        it->second.erase(action_type);
}

bool ActionLocksManager::hasAny(const StoragePtr & table) const
{
    std::lock_guard lock(mutex);

    auto it = storage_locks.find(table.get());
    if (it == storage_locks.end())
        return false;

    for (const auto & [_, action_lock] : it->second)
        if (!action_lock.expired())
            return true;

    return false;
}

bool ActionLocksManager::hasAny() const
{
    std::lock_guard lock(mutex);

    for (const auto & [_, locks] : storage_locks)
        for (const auto & type_and_lock : locks)
            if (!type_and_lock.second.expired())
                return true;

    return false;
}

void ActionLocksManager::cleanExpired()
{
    std::lock_guard lock(mutex);

    for (auto it_storage = storage_locks.begin(); it_storage != storage_locks.end();)
    {
        auto & locks = it_storage->second;
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
