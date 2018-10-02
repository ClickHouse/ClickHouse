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
}


template <typename F>
inline void forEachTable(Context & context, F && f)
{
    for (auto & elem : context.getDatabases())
        for (auto iterator = elem.second->getIterator(context); iterator->isValid(); iterator->next())
            f(iterator->table());

}

void ActionLocksManager::add(StorageActionBlockType action_type)
{
    forEachTable(global_context, [&] (const StoragePtr & table)
    {
        ActionLock action_lock = table->getActionLock(action_type);

        if (!action_lock.expired())
        {
            std::lock_guard<std::mutex> lock(mutex);
            storage_locks[table.get()][action_type] = std::move(action_lock);
        }
    });
}

void ActionLocksManager::add(const String & database_name, const String & table_name, StorageActionBlockType action_type)
{
    if (auto table = global_context.tryGetTable(database_name, table_name))
    {
        ActionLock action_lock = table->getActionLock(action_type);

        if (!action_lock.expired())
        {
            std::lock_guard<std::mutex> lock(mutex);
            storage_locks[table.get()][action_type] = std::move(action_lock);
        }
    }
}

void ActionLocksManager::remove(StorageActionBlockType action_type)
{
    std::lock_guard<std::mutex> lock(mutex);

    for (auto & storage_elem : storage_locks)
        storage_elem.second.erase(action_type);
}

void ActionLocksManager::remove(const String & database_name, const String & table_name, StorageActionBlockType action_type)
{
    if (auto table = global_context.tryGetTable(database_name, table_name))
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (storage_locks.count(table.get()))
            storage_locks[table.get()].erase(action_type);
    }
}

void ActionLocksManager::cleanExpired()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (auto it_storage = storage_locks.begin(); it_storage != storage_locks.end(); )
    {
        auto & locks = it_storage->second;
        for (auto it_lock = locks.begin(); it_lock != locks.end(); )
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
