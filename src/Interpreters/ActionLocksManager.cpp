#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ActionLockTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Common/Exception.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ActionLocks
{
    ACTION_LOCK_TYPES(ACTION_LOCK_DEFINE)
}


namespace
{
const char * getActionName(StorageActionBlockType action_type)
{
    switch (action_type)
    {
#define ACTION_NAME_CASE(ID, VALUE, NAME) case ActionLocks::ID: return NAME;
        ACTION_LOCK_TYPES(ACTION_NAME_CASE)
#undef ACTION_NAME_CASE
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown action type: {}", action_type);
    }
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
        std::lock_guard lock(mutex);
        storage_locks[table.get()][action_type] = std::move(action_lock);
    }
}

void ActionLocksManager::remove(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        remove(table, action_type);
}

void ActionLocksManager::remove(const StoragePtr & table, StorageActionBlockType action_type)
{
    std::lock_guard lock(mutex);

    if (storage_locks.contains(table.get()))
        storage_locks[table.get()].erase(action_type);
}

Names ActionLocksManager::getStoppedActions(const StoragePtr & table) const
{
    Names actions;
    {
        std::lock_guard lock(mutex);
        const auto it = storage_locks.find(table.get());
        if (it == storage_locks.end())
            return actions;

        /// Use the same key as registration. A forwarded lock is not reattributed to its target,
        /// and expired locks are only observed here, not removed ahead of `cleanExpired`.
        for (const auto & [action_type, action_lock] : it->second)
            if (!action_lock.expired())
                actions.emplace_back(getActionName(action_type));
    }
    std::ranges::sort(actions);
    return actions;
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
