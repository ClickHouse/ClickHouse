#pragma once
#include <Core/Types.h>
#include <Common/ActionLock.h>
#include <unordered_map>
#include <mutex>


namespace DB
{

class IStorage;
class Context;

/// Holds ActionLocks for tables
/// Does not store pointers to tables
class ActionLocksManager
{
public:
    explicit ActionLocksManager(Context & global_context_) : global_context(global_context_) {}

    /// Adds new locks for each table
    void add(StorageActionBlockType action_type);
    /// Add new lock for a table if it has not been already added
    void add(const String & database_name, const String & table_name, StorageActionBlockType action_type);

    /// Remove locks for all tables
    void remove(StorageActionBlockType action_type);
    /// Removes a lock for a table if it exists
    void remove(const String & database_name, const String & table_name, StorageActionBlockType action_type);

    /// Removes all locks of non-existing tables
    void cleanExpired();

private:
    Context & global_context;

    using StorageRawPtr = const IStorage *;
    using Locks = std::unordered_map<size_t, ActionLock>;
    using StorageLocks = std::unordered_map<StorageRawPtr, Locks>;

    mutable std::mutex mutex;
    StorageLocks storage_locks;
};

}
