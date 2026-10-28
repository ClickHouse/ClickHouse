#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ActionLockTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/StorageProxy.h>

#include <algorithm>
#include <vector>


namespace DB
{

namespace ActionLocks
{
    ACTION_LOCK_TYPES(ACTION_LOCK_DEFINE)
}


namespace
{
/// Null for an action type that is not in `ACTION_LOCK_TYPES`: a listing must not fail as a whole
/// because one registered type has no display name.
const char * tryGetActionName(StorageActionBlockType action_type)
{
    switch (action_type)
    {
#define ACTION_NAME_CASE(ID, VALUE, NAME) case ActionLocks::ID: return NAME;
        ACTION_LOCK_TYPES(ACTION_NAME_CASE)
#undef ACTION_NAME_CASE
        default: return nullptr;
    }
}

/// The storage that owns the blocker `getActionLock` returns. A lock lives exactly as long as that
/// storage, so keying the registry by it makes `ActionLock::expired` a complete validity check and
/// attributes the entry to the table the action actually blocks.
///
/// `load` materializes a lazy stand-in at every hop. A control command names the table, so loading is
/// its expected cost and the only way to reach the storage that really owns the blocker; a listing
/// must not load, and an unloaded stand-in has no entry to find anyway.
StoragePtr resolveActionLockOwner(const StoragePtr & table, StorageActionBlockType action_type, bool load)
{
    StoragePtr owner = table;
    /// A chain is one hop in practice. Only broken metadata can make it cyclic, so detect a repeat
    /// rather than capping the depth: a legal chain of any length still resolves to its real owner.
    std::vector<const IStorage *> seen;
    while (true)
    {
        if (load)
            owner = resolveStorageProxyLoading(owner);

        StoragePtr next = owner->getActionLockOwner(action_type);
        if (!next || next.get() == owner.get())
            return owner;

        if (std::ranges::find(seen, owner.get()) != seen.end())
            return owner;

        seen.push_back(owner.get());
        owner = std::move(next);
    }
}

/// The storage that holds the registry entry for a table as `system.tables` names it. A proxy is a
/// transparent stand-in for the same logical table, so the entry lives on the storage it wraps; an
/// `Alias` or a materialized view is a table in its own right and keeps its own (empty) entry.
/// Never loads a stand-in: a listing must not materialize a table.
StoragePtr resolveEntryOwner(const StoragePtr & table)
{
    StoragePtr owner = table;
    for (size_t depth = 0; depth < max_storage_proxy_depth && owner; ++depth)
    {
        const auto * proxy = dynamic_cast<const StorageProxy *>(owner.get());
        if (!proxy)
            break;

        auto nested = proxy->tryGetNested();
        if (!nested)
            break;

        owner = std::move(nested);
    }
    return owner;
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
    /// Resolve with loading, so the entry lands on the storage that owns the blocker even when a
    /// forwarding storage points at a lazy stand-in; then take the lock from that same storage, so
    /// the key and the lock cannot diverge even if the forwarding chain changes in between. For every
    /// forwarding storage `getActionLock` forwards the same way, and the actions that act on a
    /// refresher instead return no lock, so this is the same call the named storage would make.
    StoragePtr owner = resolveActionLockOwner(table, action_type, /*load=*/true);

    ActionLock action_lock = owner->getActionLock(action_type);

    if (action_lock.expired())
        return;

    std::lock_guard lock(mutex);
    storage_locks[owner.get()][action_type] = std::move(action_lock);
}

void ActionLocksManager::remove(const StorageID & table_id, StorageActionBlockType action_type)
{
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext()))
        remove(table, action_type);
}

void ActionLocksManager::remove(const StoragePtr & table, StorageActionBlockType action_type)
{
    /// Resolve the same way `add` did, so a control issued through any forwarding storage releases
    /// the entry that the target owns. Without loading: a global `SYSTEM START` walks every table and
    /// must not materialize a lazy stand-in, which has no entry to release anyway.
    StoragePtr owner = resolveActionLockOwner(table, action_type, /*load=*/false);

    std::lock_guard lock(mutex);
    if (auto it = storage_locks.find(owner.get()); it != storage_locks.end())
        it->second.erase(action_type);
}

Names ActionLocksManager::getStoppedActions(const StoragePtr & table) const
{
    Names actions;
    if (!table)
        return actions;

    /// A proxy is the same logical table as the storage it wraps, and the entry lives on that storage,
    /// so resolve it here. `resolveEntryOwner` does not load an unloaded stand-in, which could not have
    /// an entry anyway.
    StoragePtr owner = resolveEntryOwner(table);

    {
        std::lock_guard lock(mutex);
        /// Entries are keyed by the blocker owner, so this reports the actions of this exact
        /// instance. A forwarding storage holds none of its own: its lock is registered on the
        /// table it forwards to, which is the table the action actually blocks.
        const auto it = storage_locks.find(owner.get());
        if (it == storage_locks.end())
            return actions;

        /// Expired locks are only observed here, not removed ahead of `cleanExpired`.
        for (const auto & [action_type, action_lock] : it->second)
            if (!action_lock.expired())
                if (const char * name = tryGetActionName(action_type))
                    actions.emplace_back(name);
    }
    std::ranges::sort(actions);
    return actions;
}

void ActionLocksManager::cleanExpired()
{
    std::lock_guard lock(mutex);

    for (auto it_storage = storage_locks.begin(); it_storage != storage_locks.end();)
    {
        auto & locks = it_storage->second;
        for (auto it_lock = locks.begin(); it_lock != locks.end();)
        {
            if (it_lock->second.expired())
                it_lock = locks.erase(it_lock);
            else
                ++it_lock;
        }

        if (locks.empty())
            it_storage = storage_locks.erase(it_storage);
        else
            ++it_storage;
    }
}

}
