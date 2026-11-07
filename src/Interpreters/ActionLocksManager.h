#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ActionLock.h>
#include <base/types.h>

#include <functional>
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

    /// Removes a lock for a table if it exists.
    /// Also removes the locks of the same type held on the storage the lock resolved to, when it was taken
    /// through a forwarding storage (`Alias`, a materialized view with an inner table), and the locks taken
    /// through forwarding storages that resolved to the specified table.
    /// With `resolve_forwarding_target`, a forwarding storage that holds no lock of this type is resolved
    /// to its current target, whose locks are removed as well. This reaches the target's database, so it is
    /// meant for a `SYSTEM START` of one explicitly named table, not for the loop over all tables.
    /// The locks of a storage other than the specified one are removed only if `may_release_owner` (when set)
    /// allows it for that storage: the caller checks its access there.
    using OwnerReleaseCheck = std::function<bool(const StoragePtr & owner)>;
    void remove(const StorageID & table_id, StorageActionBlockType action_type);
    void remove(
        const StoragePtr & table,
        StorageActionBlockType action_type,
        bool resolve_forwarding_target = false,
        const OwnerReleaseCheck & may_release_owner = {});

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

    struct LockEntry
    {
        ActionLock lock;
        /// The storage that owns the blocker behind `lock`: differs from the storage the lock is kept for
        /// when it was taken through a forwarding storage. Only compared, never dereferenced.
        StorageRawPtr owner = nullptr;
        /// The same storage, recorded when the lock was taken, to notify it on removal without
        /// resolving the forwarding storage again (its target may be remote and unavailable).
        std::weak_ptr<IStorage> owner_storage;
    };

    /// The locks of one storage by action type. Each lock keeps its own owner: the target of a forwarding
    /// storage can change between two `SYSTEM STOP`s, while the earlier lock still blocks the earlier target.
    using Locks = std::unordered_map<size_t, LockEntry>;
    using StorageLocks = std::unordered_map<StorageRawPtr, Locks>;
    /// For every owner, the storages other than the owner itself that keep locks owned by it, with the number
    /// of such locks. An entry with a zero count (left by an `add` that threw) is allowed and means nothing.
    using OwnedStorages = std::unordered_map<StorageRawPtr, std::unordered_map<StorageRawPtr, size_t>>;

    bool hasImpl(StorageRawPtr table, std::optional<StorageActionBlockType> action_type) const;
    /// The owner of the lock of this type recorded for `table`: `std::nullopt` without such a lock,
    /// null when `table` owns the lock itself or the owner no longer exists.
    std::optional<StoragePtr> tryGetRecordedOwner(StorageRawPtr table, StorageActionBlockType action_type) const;
    void unlinkOwner(StorageRawPtr storage, StorageRawPtr owner) noexcept;
    void eraseLock(StorageLocks::iterator it, Locks::iterator it_lock) noexcept;
    void removeOwnedLocks(StorageRawPtr owner, StorageActionBlockType action_type);

    mutable std::mutex mutex;
    StorageLocks storage_locks;
    OwnedStorages owned_storages;
};

}
