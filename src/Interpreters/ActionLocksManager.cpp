#include "ActionLocksManager.h"
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>


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
}


ActionLocksManager::ActionLocksManager(const Context & context)
        : global_context(context.getGlobalContext())
{
}

template <typename F>
inline void forEachTable(F && f, const Context & context)
{
    for (auto & elem : DatabaseCatalog::instance().getDatabases())
        for (auto iterator = elem.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            if (auto table = iterator->table())
                f(table);

}

void ActionLocksManager::add(StorageActionBlockType action_type, const Context & context)
{
    forEachTable([&](const StoragePtr & table) { add(table, action_type); }, context);
}

void ActionLocksManager::add(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, global_context))
        add(table, action_type);
}

void ActionLocksManager::add(const StoragePtr & table, StorageActionBlockType action_type)
{
    ActionLock action_lock = table->getActionLock(action_type);

    if (!action_lock.expired())
    {
        std::lock_guard lock(mutex);
        storage_locks[table.get()][action_type] = std::move(action_lock);
    }
}

void ActionLocksManager::remove(StorageActionBlockType action_type)
{
    std::lock_guard lock(mutex);

    for (auto & storage_elem : storage_locks)
        storage_elem.second.erase(action_type);
}

void ActionLocksManager::remove(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, global_context))
        remove(table, action_type);
}

void ActionLocksManager::remove(const StoragePtr & table, StorageActionBlockType action_type)
{
    std::lock_guard lock(mutex);

    if (storage_locks.count(table.get()))
        storage_locks[table.get()].erase(action_type);
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
