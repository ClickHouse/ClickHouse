#pragma once

#include <common/types.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ActionLock.h>
#include <Interpreters/StorageID.h>

#include <mutex>
#include <unordered_map>


namespace DB
{

class Context;

/// Holds ActionLocks for tables
/// Does not store pointers to tables
class ActionLocksManager
{
public:
    ActionLocksManager(const Context & context);

    /// Adds new locks for each table
    void add(StorageActionBlockType action_type, const Context & context);
    /// Add new lock for a table if it has not been already added
    void add(const StorageID & table_id, StorageActionBlockType action_type);
    void add(const StoragePtr & table, StorageActionBlockType action_type);

    /// Remove locks for all tables
    void remove(StorageActionBlockType action_type);
    /// Removes a lock for a table if it exists
    void remove(const StorageID & table_id, StorageActionBlockType action_type);
    void remove(const StoragePtr & table, StorageActionBlockType action_type);

    /// Removes all locks of non-existing tables
    void cleanExpired();

private:
    using StorageRawPtr = const IStorage *;
    using Locks = std::unordered_map<size_t, ActionLock>;
    using StorageLocks = std::unordered_map<StorageRawPtr, Locks>;

    mutable std::mutex mutex;
    StorageLocks storage_locks;
    const Context & global_context;
};

}
