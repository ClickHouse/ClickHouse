#pragma once

#include <Core/Names.h>
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

    /// Returns sorted, distinct names of non-expired controls registered for this storage instance.
    Names getStoppedActions(const StoragePtr & table) const;

    /// Removes all locks of non-existing tables
    void cleanExpired();

private:
    using StorageRawPtr = const IStorage *;
    using Locks = std::unordered_map<size_t, ActionLock>;
    /// Entries are keyed by a raw pointer, so a new storage at a recycled address would otherwise
    /// observe controls registered by the previous owner. The weak pointer validates identity on
    /// read; stale entries are still only reclaimed when the blocker itself expires.
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
