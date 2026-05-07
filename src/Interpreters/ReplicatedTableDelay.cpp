#include <algorithm>
#include <queue>

#include <Common/logger_useful.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ReplicatedTableDelay.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

/// Traverse referential dependencies of a storage (e.g. View -> ReplicatedMergeTree)
/// and aggregate delay/readonly info across all discovered ReplicatedMergeTree tables.
ReplicatedTableDelay getReplicatedDelay(const StoragePtr & storage, ContextPtr context)
{
    if (const auto replicated_storage = castStorage<StorageReplicatedMergeTree>(storage, DeferredTable::Load))
        return {
            .max_absolute_delay = replicated_storage->getAbsoluteDelay(),
            .is_replicated = true,
            .is_readonly = replicated_storage->isTableReadOnly() };

    if (castStorage<MergeTreeData>(storage, DeferredTable::Load))
        return ReplicatedTableDelay{};

    const auto & logger = getLogger("ReplicatedTableDelay");
    const auto & catalog = DatabaseCatalog::instance();
    ReplicatedTableDelay info;

    LOG_DEBUG(logger, "Checking replication delay of {} and its dependencies", storage->getStorageID().getNameForLogs());

    /// Do a breadth-first search on all dependencies (direct and transitive) and get the maximum replication delay
    /// No need for a visited set because ClickHouse does not allow cyclical dependencies and graph should be small
    std::queue<StoragePtr> queue({ storage });
    while (!queue.empty())
    {
        auto current = queue.front(); queue.pop();
        if (!current)
            continue;

        if (const auto replicated = castStorage<StorageReplicatedMergeTree>(current, DeferredTable::Load))
        {
            auto delay = replicated->getAbsoluteDelay();
            info.is_replicated = true;
            info.max_absolute_delay = std::max(info.max_absolute_delay, delay);
            info.is_readonly = info.is_readonly || replicated->isTableReadOnly();
            LOG_TRACE(logger, "Found ReplicatedMergeTree dependency {} with delay of {} seconds",
                current->getStorageID().getNameForLogs(), delay);
        }

        for (const auto & dependency : catalog.getReferentialDependencies(current->getStorageID()))
            queue.push(catalog.tryGetTable(dependency, context));
    }

    LOG_DEBUG(logger, "Replication delay of {} is {} seconds", storage->getStorageID().getNameForLogs(), info.max_absolute_delay);
    return info;
}

}
