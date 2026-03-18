#include "RWLock.h"
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace ProfileEvents
{
    extern const Event RWLockAcquiredReadLocks;
    extern const Event RWLockAcquiredWriteLocks;
    extern const Event RWLockReadersWaitMilliseconds;
    extern const Event RWLockWritersWaitMilliseconds;
}


namespace CurrentMetrics
{
    extern const Metric RWLockWaitingReaders;
    extern const Metric RWLockWaitingWriters;
    extern const Metric RWLockActiveReaders;
    extern const Metric RWLockActiveWriters;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** A one-time-use-object that represents lock ownership
  * For the purpose of exception safety guarantees LockHolder is to be used in two steps:
  *   1. Create an instance (allocating all the needed memory)
  *   2. Associate the instance with the lock (attach to the lock and locking request group)
  */
class RWLockImpl::LockHolderImpl
{
    bool bound{false};
    String query_id;
    CurrentMetrics::Increment active_client_increment;
    RWLock parent;
    GroupsContainer::iterator it_group;

public:
    LockHolderImpl(const LockHolderImpl & other) = delete;
    LockHolderImpl& operator=(const LockHolderImpl & other) = delete;

    /// Implicit memory allocation for query_id is done here
    LockHolderImpl(const String & query_id_, Type type)
        : query_id{query_id_}
        , active_client_increment{
            type == Type::Read ? CurrentMetrics::RWLockActiveReaders : CurrentMetrics::RWLockActiveWriters}
    {
    }

    ~LockHolderImpl()
    {
        if (bound && parent != nullptr)
            parent->unlock(it_group, query_id);
        else
            active_client_increment.destroy();
    }

private:
    /// A separate method which binds the lock holder to the owned lock
    /// N.B. It is very important that this method produces no allocations
    bool bindWith(RWLock && parent_, GroupsContainer::iterator it_group_) noexcept
    {
        if (bound || parent_ == nullptr)
            return false;
        it_group = it_group_;
        parent = std::move(parent_);
        ++it_group->requests;
        bound = true;
        return true;
    }

    friend class RWLockImpl;
};


/** General algorithm:
  *   Step 1. Try the FastPath (for both Reads/Writes)
  *   Step 2. Find ourselves request group: attach to existing or create a new one
  *   Step 3. Wait/timed wait for ownership signal
  *   Step 3a. Check if we must handle timeout and exit
  *   Step 4. Persist lock ownership
  *
  * To guarantee that we do not get any piece of our data corrupted:
  *   1. Perform all actions that include allocations before changing lock's internal state
  *   2. Roll back any changes that make the state inconsistent
  *
  * Note: "SM" in the commentaries below stands for STATE MODIFICATION
  */
RWLockImpl::LockHolder
RWLockImpl::getLock(RWLockImpl::Type type, const String & query_id, const std::chrono::milliseconds & lock_timeout_ms, bool throw_in_fast_path)
{
    const auto lock_deadline_tp =
        (lock_timeout_ms == std::chrono::milliseconds(0))
            ? std::chrono::time_point<std::chrono::steady_clock>::max()
            : std::chrono::steady_clock::now() + lock_timeout_ms;

    const bool request_has_query_id = query_id != NO_QUERY;

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    CurrentMetrics::Increment waiting_client_increment((type == Read) ? CurrentMetrics::RWLockWaitingReaders
                                                                      : CurrentMetrics::RWLockWaitingWriters);
    auto finalize_metrics = [type, &watch] ()
    {
        ProfileEvents::increment((type == Read) ? ProfileEvents::RWLockAcquiredReadLocks
                                                : ProfileEvents::RWLockAcquiredWriteLocks);
        ProfileEvents::increment((type == Read) ? ProfileEvents::RWLockReadersWaitMilliseconds
                                                : ProfileEvents::RWLockWritersWaitMilliseconds, watch.elapsedMilliseconds());
    };

    /// This object is placed above unique_lock, because it may lock in destructor.
    auto lock_holder = std::make_shared<LockHolderImpl>(query_id, type);

    std::unique_lock state_lock(internal_state_mtx);

    /// The FastPath:
    /// Check if the same query_id already holds the required lock in which case we can proceed without waiting
    if (request_has_query_id)
    {
        const auto owner_query_it = owner_queries.find(query_id);
        if (owner_query_it != owner_queries.end())
        {
            if (wrlock_owner != writers_queue.end())
            {
                if (throw_in_fast_path)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "RWLockImpl::getLock(): RWLock is already locked in exclusive mode");
                return nullptr;
            }

            /// Lock upgrading is not supported
            if (type == Write)
            {
                if (throw_in_fast_path)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "RWLockImpl::getLock(): Cannot acquire exclusive lock while RWLock is already locked");
                return nullptr;
            }

            /// N.B. Type is Read here, query_id is not empty and it_query is a valid iterator
            ++owner_query_it->second;                                  /// SM1: nothrow
            lock_holder->bindWith(shared_from_this(), rdlock_owner);   /// SM2: nothrow

            finalize_metrics();
            return lock_holder;
        }
    }

    if (type == Type::Write)
    {
        /// Always add a group for a writer (writes are never performed simultaneously).
        writers_queue.emplace_back(type);  /// SM1: may throw (nothing to roll back)
    }
    else
    {
        /// We don't always add a group to readers_queue here because multiple readers can use the same group.
        /// We can reuse the last group if the last group didn't get ownership yet,
        /// or even if it got ownership but there are no writers waiting in writers_queue.
        bool can_use_last_group = !readers_queue.empty() && (!readers_queue.back().ownership || writers_queue.empty());

        if (!can_use_last_group)
            readers_queue.emplace_back(type);  /// SM1: may throw (nothing to roll back)
    }

    GroupsContainer::iterator it_group =
            (type == Type::Write) ? std::prev(writers_queue.end()) : std::prev(readers_queue.end());

    /// Lock is free to acquire
    if (rdlock_owner == readers_queue.end() && wrlock_owner == writers_queue.end())
    {
        /// Set `rdlock_owner` or `wrlock_owner` and make it owner.
        (type == Read ? rdlock_owner : wrlock_owner) = it_group;  /// SM2: nothrow
        grantOwnership(it_group);
    }
    else
    {
        /// Wait until our group becomes the lock owner
        const auto predicate = [&] () { return it_group->ownership; };

        if (lock_deadline_tp == std::chrono::time_point<std::chrono::steady_clock>::max())
        {
            ++it_group->requests;
            it_group->cv.wait(state_lock, predicate);
            --it_group->requests;
        }
        else
        {
            ++it_group->requests;
            const auto wait_result = it_group->cv.wait_until(state_lock, lock_deadline_tp, predicate);
            --it_group->requests;

            /// Step 3a. Check if we must handle timeout and exit
            if (!wait_result)  /// Wait timed out!
            {
                /// Rollback(SM1): nothrow
                if (it_group->requests == 0)
                {
                    ((type == Read) ? readers_queue : writers_queue).erase(it_group);
                }
                /// While we were waiting for this write lock (which has just failed) more readers could start waiting,
                /// we need to wake up them now.
                if ((rdlock_owner != readers_queue.end()) && writers_queue.empty())
                    grantOwnershipToAllReaders();
                return nullptr;
            }
        }
    }

    /// Our group must be an owner here.
    chassert(it_group->ownership);

    if (request_has_query_id)
    {
        try
        {
            const auto emplace_res =
                    owner_queries.emplace(query_id, 1);    /// SM2: may throw on insertion
            if (!emplace_res.second)
                ++emplace_res.first->second;               /// SM3: nothrow
        }
        catch (...)
        {
            /// Methods std::list<>::emplace_back() and std::unordered_map<>::emplace() provide strong exception safety
            /// We only need to roll back the changes to these objects: owner_queries and the readers/writers queue
            if (it_group->requests == 0)
                dropOwnerGroupAndPassOwnership(it_group);  /// Rollback(SM1): nothrow

            throw;
        }
    }

    lock_holder->bindWith(shared_from_this(), it_group);  /// SM: nothrow

    finalize_metrics();
    return lock_holder;
}


/** The sequence points of acquiring lock ownership by an instance of LockHolderImpl:
  *   1. owner_queries is updated
  *   2. request group is updated by LockHolderImpl which in turn becomes "bound"
  *
  * If by the time when destructor of LockHolderImpl is called the instance has been "bound",
  * it is guaranteed that all three steps have been executed successfully and the resulting state is consistent.
  * With the mutex locked the order of steps to restore the lock's state can be arbitrary
  *
  * We do not employ try-catch: if something bad happens and chassert() is disabled, there is nothing we can do
  * (we can't throw an exception here because RWLockImpl::unlock() is called from the destructor ~LockHolderImpl).
  */
void RWLockImpl::unlock(GroupsContainer::iterator group_it, const String & query_id) noexcept
{
    std::lock_guard state_lock(internal_state_mtx);

    /// Our group must be an owner here.
    if (!group_it->ownership)
    {
        chassert(false && "RWLockImpl::unlock() is called for a non-owner group");
        return;
    }

    /// Check consistency.
    if ((group_it->type == Read)
            ? !(rdlock_owner != readers_queue.end() && wrlock_owner == writers_queue.end())
            : !(wrlock_owner != writers_queue.end() && rdlock_owner == readers_queue.end() && group_it == wrlock_owner))
    {
        chassert(false && "RWLockImpl::unlock() found the rwlock inconsistent");
        return;
    }

    /// If query_id is not empty it must be listed in parent->owner_queries
    if (query_id != NO_QUERY)
    {
        const auto owner_query_it = owner_queries.find(query_id);
        if (owner_query_it != owner_queries.end())
        {
            if (--owner_query_it->second == 0)          /// SM: nothrow
                owner_queries.erase(owner_query_it);    /// SM: nothrow
        }
    }

    /// If we are the last remaining referrer, remove this QNode and notify the next one
    if (--group_it->requests == 0)               /// SM: nothrow
        dropOwnerGroupAndPassOwnership(group_it);
}


void RWLockImpl::dropOwnerGroupAndPassOwnership(GroupsContainer::iterator group_it) noexcept
{
    /// All readers with ownership must finish before switching to write phase.
    /// Such readers has iterators from `readers_queue.begin()` to `rdlock_owner`, so if `rdlock_owner` is equal to `readers_queue.begin()`
    /// that means there is only one reader with ownership left in the readers_queue and we can proceed to generic procedure.
    if ((group_it->type == Read) && (rdlock_owner != readers_queue.begin()) && (rdlock_owner != readers_queue.end()))
    {
        if (rdlock_owner == group_it)
            --rdlock_owner;
        readers_queue.erase(group_it);
        /// If there are no writers waiting in writers_queue then we can wake up other readers.
        if (writers_queue.empty())
            grantOwnershipToAllReaders();
        return;
    }

    rdlock_owner = readers_queue.end();
    wrlock_owner = writers_queue.end();

    if (group_it->type == Read)
    {
        readers_queue.erase(group_it);
        /// Prepare next phase
        if (!writers_queue.empty())
            wrlock_owner = writers_queue.begin();
        else
            rdlock_owner = readers_queue.begin();
    }
    else
    {
        writers_queue.erase(group_it);
        /// Prepare next phase
        if (!readers_queue.empty())
            rdlock_owner = readers_queue.begin();
        else
            wrlock_owner = writers_queue.begin();
    }

    if (rdlock_owner != readers_queue.end())
    {
        grantOwnershipToAllReaders();
    }
    else if (wrlock_owner != writers_queue.end())
    {
        grantOwnership(wrlock_owner);
    }
}


void RWLockImpl::grantOwnership(GroupsContainer::iterator group_it) noexcept
{
    if (!group_it->ownership)
    {
        group_it->ownership = true;
        group_it->cv.notify_all();
    }
}


void RWLockImpl::grantOwnershipToAllReaders() noexcept
{
    if (rdlock_owner != readers_queue.end())
    {
        size_t num_new_owners = 0;

        for (;;)
        {
            if (!rdlock_owner->ownership)
                ++num_new_owners;
            grantOwnership(rdlock_owner);
            if (std::next(rdlock_owner) == readers_queue.end())
                break;
            ++rdlock_owner;
        }

        /// There couldn't be more than one reader group which is not an owner.
        /// (Because we add a new reader group only if the last reader group is already an owner - see the `can_use_last_group` variable.)
        chassert(num_new_owners <= 1);
    }
}


std::unordered_map<String, size_t> RWLockImpl::getOwnerQueryIds() const
{
    std::lock_guard lock{internal_state_mtx};
    return owner_queries;
}


String RWLockImpl::getOwnerQueryIdsDescription() const
{
    auto map = getOwnerQueryIds();
    WriteBufferFromOwnString out;
    bool need_comma = false;
    for (const auto & [query_id, num_owners] : map)
    {
        if (need_comma)
            out << ", ";
        out << query_id;
        if (num_owners != 1)
            out << " (" << num_owners << ")";
        need_comma = true;
    }
    return out.str();
}

}
