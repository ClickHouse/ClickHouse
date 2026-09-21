#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ActionLock.h>
#include <base/types.h>

#include <mutex>
#include <unordered_map>


namespace DB
{

/// Holds `ActionLock` objects without keeping their storage instances alive.
class ActionLocksManager : WithContext
{
public:
    explicit ActionLocksManager(ContextPtr context);

    /// Add new lock for a table if it has not been already added
    void add(const StorageID & table_id, StorageActionBlockType action_type);
    void add(const StoragePtr & table, StorageActionBlockType action_type);

    /// Removes a lock for a table if it exists
    void remove(const StorageID & table_id, StorageActionBlockType action_type);
    void remove(const StoragePtr & table, StorageActionBlockType action_type);

    /// Removes all locks of non-existing tables
    void cleanExpired();

private:
    using StorageRawPtr = const IStorage *;
    using Locks = std::unordered_map<size_t, ActionLock>;
    struct StorageEntry
    {
        std::weak_ptr<IStorage> storage;
        Locks locks;

        bool belongsTo(const StoragePtr & table) const
        {
            return !storage.owner_before(table) && !table.owner_before(storage);
        }
    };
    using StorageLocks = std::unordered_map<StorageRawPtr, StorageEntry>;

    mutable std::mutex mutex;
    StorageLocks storage_locks;
};

}
