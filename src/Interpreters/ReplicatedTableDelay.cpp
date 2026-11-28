#include <algorithm>
#include <queue>
#include <unordered_set>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ReplicatedTableDelay.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageView.h>

namespace DB
{

namespace
{

/// The tables that `storage` reads as data sources. Only storages that forward a read to other
/// local tables are expanded: a `View`, a `MaterializedView` and wrappers like `Alias`.
/// Anything else (`Distributed`, `Merge`, `Dictionary`, ...) is not followed.
std::vector<StoragePtr> getStoragesRead(const StoragePtr & storage, const ContextPtr & context)
{
    if (const auto materialized_view = castStorage<StorageMaterializedView>(storage, DeferredTable::Load))
        return {materialized_view->tryGetTargetTable()};

    if (castStorage<StorageView>(storage, DeferredTable::Load))
    {
        const auto & catalog = DatabaseCatalog::instance();
        std::vector<StoragePtr> result;
        for (const auto & dependency : catalog.getReferentialDependencies(storage->getStorageID()))
            result.push_back(catalog.tryGetTable(dependency, context));
        return result;
    }

    return storage->getUnderlyingStorages();
}

}

ReplicatedTableDelay getReplicatedDelay(const StoragePtr & storage, ContextPtr context)
{
    ReplicatedTableDelay info;
    std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> visited;
    std::queue<StoragePtr> queue;

    auto enqueue = [&](const StoragePtr & table)
    {
        if (table && visited.insert(table->getStorageID()).second)
            queue.push(table);
    };

    enqueue(storage);
    while (!queue.empty())
    {
        auto current = queue.front();
        queue.pop();

        if (const auto replicated = castStorage<StorageReplicatedMergeTree>(current, DeferredTable::Load))
        {
            info.is_replicated = true;
            info.max_absolute_delay = std::max(info.max_absolute_delay, replicated->getAbsoluteDelay());
            info.is_readonly = info.is_readonly || replicated->isTableReadOnly();
            continue;
        }

        for (const auto & table : getStoragesRead(current, context))
            enqueue(table);
    }

    return info;
}

}
