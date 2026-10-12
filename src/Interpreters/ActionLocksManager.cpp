#include <Interpreters/ActionLocksManager.h>
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

const char * getActionName(StorageActionBlockType action_type)
{
    switch (action_type)
    {
        case ActionLocks::PartsMerge: return "merges";
        case ActionLocks::PartsFetch: return "fetches";
        case ActionLocks::PartsSend: return "replicated_sends";
        case ActionLocks::ReplicationQueue: return "replication_queue";
        case ActionLocks::DistributedSend: return "distributed_sends";
        case ActionLocks::PartsTTLMerge: return "ttl_merges";
        case ActionLocks::PartsMove: return "moves";
        case ActionLocks::PullReplicationLog: return "pull_replication_log";
        case ActionLocks::Cleanup: return "cleanup";
        case ActionLocks::ViewRefresh: return "view_refresh";
        case ActionLocks::VirtualPartsUpdate: return "virtual_parts_update";
        case ActionLocks::ReduceBlockingParts: return "reduce_blocking_parts";
        case ActionLocks::ViewRefreshPause: return "view_refresh_pause";
        case ActionLocks::StreamConsume: return "streaming_consumption";
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

    std::lock_guard lock(mutex);
    auto it = storage_locks.find(table.get());
    if (it != storage_locks.end() && !it->second.belongsTo(table))
        storage_locks.erase(it);

    if (!action_lock.expired())
    {
        auto & entry = storage_locks[table.get()];
        entry.storage = table;
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
    std::lock_guard lock(mutex);

    const auto it = storage_locks.find(table.get());
    if (it == storage_locks.end())
        return;

    if (it->second.belongsTo(table))
    {
        it->second.locks.erase(action_type);
        if (it->second.locks.empty())
            storage_locks.erase(it);
    }
    else
        storage_locks.erase(it);
}

Names ActionLocksManager::getStoppedActions(const StoragePtr & table) const
{
    Names actions;
    {
        std::lock_guard lock(mutex);
        const auto it = storage_locks.find(table.get());
        if (it == storage_locks.end() || !it->second.belongsTo(table))
            return actions;

        for (const auto & [action_type, action_lock] : it->second.locks)
        {
            /// Expired locks may remain until the next control query calls `cleanExpired`.
            if (!action_lock.expired())
                actions.emplace_back(getActionName(action_type));
        }
    }

    std::ranges::sort(actions);
    actions.erase(std::unique(actions.begin(), actions.end()), actions.end());
    return actions;
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
