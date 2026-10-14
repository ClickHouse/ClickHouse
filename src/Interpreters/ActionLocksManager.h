#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ActionLock.h>
#include <base/types.h>

#include <mutex>
#include <optional>
#include <memory>
#include <unordered_map>
#include <unordered_set>


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

    /// Checks whether there is a lock for specified table and action_type.
    /// Also counts a lock taken through a forwarding storage (`Alias`, a materialized view with an inner table)
    /// that resolved to the specified table.
    bool has(const StorageID & table_id, StorageActionBlockType action_type) const;
    bool has(const StoragePtr & table, StorageActionBlockType action_type) const;

    /// Checks whether there is a lock for specified table and any action_type
    bool hasAny(const StorageID & table_id) const;
    bool hasAny(const StoragePtr & table) const;

    /// Removes all locks of non-existing tables
    void cleanExpired();

private:
    using StorageRawPtr = const IStorage *;
    using Locks = std::unordered_map<size_t, ActionLock>;

    struct StorageLocksEntry
    {
        /// The storage that owns the blockers behind `locks`: differs from the key when the locks
        /// were taken through a forwarding storage. Only compared, never dereferenced.
        StorageRawPtr owner = nullptr;
        /// The same storage, recorded when the lock was taken, to notify it on removal without
        /// resolving the forwarding storage again (its target may be remote and unavailable).
        std::weak_ptr<IStorage> owner_storage;
        Locks locks;
    };

    using StorageLocks = std::unordered_map<StorageRawPtr, StorageLocksEntry>;
    /// The keys of `storage_locks` grouped by `StorageLocksEntry::owner`.
    using OwnedStorages = std::unordered_map<StorageRawPtr, std::unordered_set<StorageRawPtr>>;

    bool hasImpl(StorageRawPtr table, std::optional<StorageActionBlockType> action_type) const;
    void eraseEntry(StorageLocks::iterator it);

    mutable std::mutex mutex;
    StorageLocks storage_locks;
    OwnedStorages owned_storages;
};

}
