#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ActionLock.h>
#include <base/types.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

/// Holds ActionLocks for tables
/// Does not store pointers to tables
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

    /// Read the existing registry without loading a stand-in or changing any controls.
    /// Reports the actions registered for this storage instance, which is the table whose blocker
    /// they hold: a forwarding storage reports nothing and its target reports the action.
    Names getStoppedActions(const StoragePtr & table) const;

    /// Removes all locks of non-existing tables
    void cleanExpired();

private:
    using StorageRawPtr = const IStorage *;
    using Locks = std::unordered_map<size_t, ActionLock>;

    /// Keyed by the storage that owns the blocker, so an entry lives exactly as long as that
    /// storage and `ActionLock::expired` alone tells whether it still belongs to a live table.
    using StorageLocks = std::unordered_map<StorageRawPtr, Locks>;

    mutable std::mutex mutex;
    StorageLocks storage_locks;
};

}
