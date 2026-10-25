#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/typeid_cast.h>

#include <vector>


namespace DB
{

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

/// Forwarding storages return the action locks of the storage they resolve to, so the lock
/// belongs to that storage rather than to the one it was requested from.
StoragePtr resolveActionLockOwner(StoragePtr table)
{
    /// Bound the walk in case of a cycle of aliases.
    for (size_t depth = 0; depth < 16 && table; ++depth)
    {
        StoragePtr next;
        if (const auto * alias = typeid_cast<const StorageAlias *>(table.get()))
            next = alias->tryGetTargetTable();
        else if (const auto * view = typeid_cast<const StorageMaterializedView *>(table.get()); view && view->hasInnerTable())
            next = view->tryGetTargetTable();

        if (!next)
            break;
        table = std::move(next);
    }
    return table;
}

/// The storage a chain of aliases resolves to, or the storage itself when it is not an alias. Null when an
/// alias in the chain has no target.
StoragePtr resolveAliases(StoragePtr table)
{
    /// Bound the walk in case of a cycle of aliases.
    for (size_t depth = 0; depth < 16 && table; ++depth)
    {
        const auto * alias = typeid_cast<const StorageAlias *>(table.get());
        if (!alias)
            break;
        table = alias->tryGetTargetTable();
    }
    return table;
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
    /// Take the lock and determine its owner from one resolution of the aliases. `StorageAlias::getActionLock`
    /// resolves its target by name on its own, so asking the alias for the lock and resolving the owner
    /// separately could pair the lock with another storage: if `EXCHANGE TABLES t AND oldt` lands between
    /// the two lookups for an alias to `t`, the lock is taken on the storage now named `oldt` while the
    /// storage now named `t` is recorded as its owner, and `SYSTEM START MERGES oldt` does not release it.
    /// The inner table of a materialized view is bound to the view by its UUID, so the view itself is asked:
    /// it also stops or pauses its refresher for the corresponding lock types.
    const auto lock_source = resolveAliases(table);
    if (!lock_source)
        return;

    ActionLock action_lock = lock_source->getActionLock(action_type);

    if (action_lock.expired())
        return;

    auto owner = resolveActionLockOwner(lock_source);
    if (!owner)
        owner = table;

    std::lock_guard lock(mutex);

    /// Allocate every node first. An exception here leaves at most empty nodes, which every reader skips
    /// and `cleanExpired` removes, so both indexes stay consistent.
    auto & locks = storage_locks[table.get()];
    size_t * owned_count = owner != table ? &owned_storages[owner.get()][table.get()] : nullptr;
    auto [it_lock, inserted] = locks.try_emplace(action_type);

    /// Nothing below throws. Count the new owner before unlinking the replaced one: when they are the same,
    /// the count does not drop to zero and `owned_count` stays valid.
    if (owned_count)
        ++*owned_count;
    if (!inserted)
        unlinkOwner(table.get(), it_lock->second.owner);

    it_lock->second.lock = std::move(action_lock);
    it_lock->second.owner = owner.get();
    it_lock->second.owner_storage = owner;
}

void ActionLocksManager::remove(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        remove(table, action_type);
}

void ActionLocksManager::remove(
    const StoragePtr & table, StorageActionBlockType action_type, bool resolve_forwarding_target, const OwnerReleaseCheck & may_release_owner)
{
    /// The blocker behind a lock taken through a forwarding storage belongs to the storage it resolved to,
    /// so `SYSTEM START` on either of them has to release it. The owner is the one recorded when the lock
    /// was taken, so a forwarding storage that holds a lock is not resolved again here: its target may be
    /// unavailable. Only a forwarding storage without a lock of this type is resolved, and only on request:
    /// `SYSTEM STOP MERGES t` followed by `SYSTEM START MERGES alias_to_t` has to release the lock on `t`.
    StoragePtr owner;
    if (auto recorded_owner = tryGetRecordedOwner(table.get(), action_type))
        owner = std::move(*recorded_owner);
    else if (resolve_forwarding_target)
        owner = resolveActionLockOwner(table);

    if (owner == table)
        owner.reset();

    /// Releasing the locks of another storage needs the caller's permission on that storage too: having
    /// the access to an `Alias` must not be enough to release a `SYSTEM STOP` taken on its target.
    /// The check runs outside of `mutex`, and the owner it approved is compared with the recorded one below.
    if (owner && may_release_owner && !may_release_owner(owner))
        owner.reset();

    bool owner_released = false;

    {
        std::lock_guard lock(mutex);

        StorageRawPtr recorded_owner_ptr = nullptr;
        if (auto it = storage_locks.find(table.get()); it != storage_locks.end())
        {
            if (auto it_lock = it->second.find(action_type); it_lock != it->second.end())
            {
                recorded_owner_ptr = it_lock->second.owner;
                eraseLock(it, it_lock);
            }
        }

        /// The locks taken through forwarding storages that resolved to this table.
        removeOwnedLocks(table.get(), action_type);

        /// A concurrent `SYSTEM STOP` may have replaced the lock with one owned by another storage,
        /// which the check above did not approve.
        if (owner && (!recorded_owner_ptr || recorded_owner_ptr == owner.get()))
        {
            /// The lock held on the owner itself, and the ones taken through other forwarding storages.
            if (auto it = storage_locks.find(owner.get()); it != storage_locks.end())
                if (auto it_lock = it->second.find(action_type); it_lock != it->second.end())
                    eraseLock(it, it_lock);
            removeOwnedLocks(owner.get(), action_type);
            owner_released = true;
        }
    }

    /// The callers notify the storage they were given; when the blocker belongs to another storage,
    /// notify it as well, so that it resumes the released action.
    if (owner_released)
        owner->onActionLockRemove(action_type);
}

bool ActionLocksManager::has(const StorageID & table_id, StorageActionBlockType action_type) const
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        return has(table, action_type);
    return false;
}

bool ActionLocksManager::has(const StoragePtr & table, StorageActionBlockType action_type) const
{
    return hasImpl(table.get(), action_type);
}

bool ActionLocksManager::hasAny(const StorageID & table_id) const
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        return hasAny(table);
    return false;
}

bool ActionLocksManager::hasAny(const StoragePtr & table) const
{
    return hasImpl(table.get(), std::nullopt);
}

bool ActionLocksManager::hasImpl(StorageRawPtr table, std::optional<StorageActionBlockType> action_type) const
{
    std::lock_guard lock(mutex);

    /// With `owner`, only the locks owned by it count.
    auto has_lock = [&](StorageRawPtr storage, StorageRawPtr owner)
    {
        auto it = storage_locks.find(storage);
        if (it == storage_locks.end())
            return false;

        for (const auto & [type, entry] : it->second)
            if ((!action_type || type == *action_type) && (!owner || entry.owner == owner) && !entry.lock.expired())
                return true;
        return false;
    };

    if (has_lock(table, nullptr))
        return true;

    if (auto it_owned = owned_storages.find(table); it_owned != owned_storages.end())
        for (const auto & [storage, count] : it_owned->second)
            if (count && has_lock(storage, table))
                return true;

    return false;
}

std::optional<StoragePtr> ActionLocksManager::tryGetRecordedOwner(StorageRawPtr table, StorageActionBlockType action_type) const
{
    std::lock_guard lock(mutex);
    auto it = storage_locks.find(table);
    if (it == storage_locks.end())
        return std::nullopt;
    auto it_lock = it->second.find(action_type);
    if (it_lock == it->second.end())
        return std::nullopt;
    if (it_lock->second.owner == table)
        return StoragePtr{};
    /// Null when the owner no longer exists, so its locks are left to `cleanExpired`.
    return it_lock->second.owner_storage.lock();
}

void ActionLocksManager::unlinkOwner(StorageRawPtr storage, StorageRawPtr owner) noexcept
{
    if (owner == storage)
        return;

    auto it_owned = owned_storages.find(owner);
    if (it_owned == owned_storages.end())
        return;

    if (auto it_count = it_owned->second.find(storage); it_count != it_owned->second.end() && --it_count->second == 0)
        it_owned->second.erase(it_count);

    if (it_owned->second.empty())
        owned_storages.erase(it_owned);
}

void ActionLocksManager::eraseLock(StorageLocks::iterator it, Locks::iterator it_lock) noexcept
{
    unlinkOwner(it->first, it_lock->second.owner);
    it->second.erase(it_lock);
}

void ActionLocksManager::removeOwnedLocks(StorageRawPtr owner, StorageActionBlockType action_type)
{
    auto it_owned = owned_storages.find(owner);
    if (it_owned == owned_storages.end())
        return;

    /// `eraseLock` modifies the index being iterated, so take the storages out of it first.
    std::vector<StorageRawPtr> storages;
    storages.reserve(it_owned->second.size());
    for (const auto & [storage, count] : it_owned->second)
        storages.push_back(storage);

    for (StorageRawPtr storage : storages)
    {
        auto it = storage_locks.find(storage);
        if (it == storage_locks.end())
            continue;
        if (auto it_lock = it->second.find(action_type); it_lock != it->second.end() && it_lock->second.owner == owner)
            eraseLock(it, it_lock);
    }
}

void ActionLocksManager::cleanExpired()
{
    std::lock_guard lock(mutex);

    for (auto it_storage = storage_locks.begin(); it_storage != storage_locks.end();)
    {
        auto & locks = it_storage->second;
        for (auto it_lock = locks.begin(); it_lock != locks.end();)
        {
            if (it_lock->second.lock.expired())
                eraseLock(it_storage, it_lock++);
            else
                ++it_lock;
        }

        if (locks.empty())
            it_storage = storage_locks.erase(it_storage);
        else
            ++it_storage;
    }

    /// The empty nodes an `add` that threw may have left.
    for (auto it_owned = owned_storages.begin(); it_owned != owned_storages.end();)
    {
        std::erase_if(it_owned->second, [](const auto & storage_and_count) { return storage_and_count.second == 0; });
        if (it_owned->second.empty())
            it_owned = owned_storages.erase(it_owned);
        else
            ++it_owned;
    }
}

}
