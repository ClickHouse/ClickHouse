#include "RWLock.h"
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>


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
    extern const int DEADLOCK_AVOIDED;
}


/** A single-use object that represents lock's ownership
  * For the purpose of exception safety guarantees LockHolder is to be used in two steps:
  *   1. Create an instance (allocating all the memory needed)
  *   2. Associate the instance with the lock (attach to the lock and locking request group)
  */
class RWLockImpl::LockHolderImpl
{
    bool bound{false};
    Type lock_type;
    String query_id;
    CurrentMetrics::Increment active_client_increment;
    RWLock parent;
    GroupsContainer::iterator it_group;

public:
    LockHolderImpl(const LockHolderImpl & other) = delete;
    LockHolderImpl& operator=(const LockHolderImpl & other) = delete;

    /// Implicit memory allocation for query_id is done here
    LockHolderImpl(const String & query_id_, Type type)
        : lock_type{type}, query_id{query_id_},
          active_client_increment{
            type == Type::Read ? CurrentMetrics::RWLockActiveReaders : CurrentMetrics::RWLockActiveWriters}
    {
    }

    ~LockHolderImpl();

private:
    /// A separate method which binds the lock holder to the owned lock
    /// N.B. It is very important that this method produces no allocations
    bool bind_with(RWLock && parent_, GroupsContainer::iterator it_group_) noexcept
    {
        if (bound)
            return false;
        it_group = it_group_;
        parent = std::move(parent_);
        ++it_group->refererrs;
        bound = true;
        return true;
    }

    friend class RWLockImpl;
};


namespace
{
    /// Global information about all read locks that query has. It is needed to avoid some type of deadlocks.

    class QueryLockInfo
    {
    private:
        mutable std::mutex mutex;
        std::map<std::string, size_t> queries;

    public:
        void add(const String & query_id)
        {
            std::lock_guard lock(mutex);

            const auto res = queries.emplace(query_id, 1);  // may throw
            if (!res.second)
                ++res.first->second;
        }

        void remove(const String & query_id) noexcept
        {
            std::lock_guard lock(mutex);

            const auto query_it = queries.find(query_id);
            if (query_it != queries.cend() && --query_it->second == 0)
                queries.erase(query_it);
        }

        void check(const String & query_id) const
        {
            std::lock_guard lock(mutex);

            if (queries.find(query_id) != queries.cend())
                throw Exception("Possible deadlock avoided. Client should retry.", ErrorCodes::DEADLOCK_AVOIDED);
        }
    };

    QueryLockInfo all_read_locks;
}


/** To guarantee that we do not get any piece of our data corrupted:
  *   1. Perform all actions that include allocations before changing lock's internal state
  *   2. Roll back any changes that make the state inconsistent
  *
  * Note: "SM" in the commentaries below stands for STATE MODIFICATION
  */
RWLockImpl::LockHolder RWLockImpl::getLock(RWLockImpl::Type type, const String & query_id)
{
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

    std::unique_lock lock(mutex);

    /// The FastPath:
    /// Check if the same query_id already holds the required lock in which case we can proceed without waiting
    if (request_has_query_id)
    {
        const auto it_query = owner_queries.find(query_id);
        if (it_query != owner_queries.end())
        {
            const auto current_owner_group = queue.begin();

            /// XXX: it means we can't upgrade lock from read to write!
            if (type == Write)
                throw Exception(
                        "RWLockImpl::getLock(): Cannot acquire exclusive lock while RWLock is already locked",
                        ErrorCodes::LOGICAL_ERROR);

            if (current_owner_group->type == Write)
                throw Exception(
                        "RWLockImpl::getLock(): RWLock is already locked in exclusive mode",
                        ErrorCodes::LOGICAL_ERROR);

            /// N.B. Type is Read here, query_id is not empty and it_query is a valid iterator
            all_read_locks.add(query_id);                                     /// SM1: may throw on insertion (nothing to roll back)
            ++it_query->second;                                               /// SM2: nothrow
            lock_holder->bind_with(shared_from_this(), current_owner_group);  /// SM3: nothrow

            finalize_metrics();
            return lock_holder;
        }
    }

    /** If the query already has any active read lock and tries to acquire another read lock
      *  but it is not in front of the queue and has to wait, deadlock is possible:
      *
      * Example (four queries, two RWLocks - 'a' and 'b'):
      *
      *     --> time -->
      *
      * q1: ra          rb
      * q2:    wa
      * q3:       rb       ra
      * q4:          wb
      *
      * We will throw an exception instead.
      */

    if (type == Type::Write || queue.empty() || queue.back().type == Type::Write)
    {
        if (type == Type::Read && request_has_query_id && !queue.empty())
            all_read_locks.check(query_id);

        /// Create a new group of locking requests
        queue.emplace_back(type);                        /// SM1: may throw (nothing to roll back)
    }
    else if (request_has_query_id && queue.size() > 1)
        all_read_locks.check(query_id);

    GroupsContainer::iterator it_group = std::prev(queue.end());

    /// We need to reference the associated group before waiting to guarantee
    /// that this group does not get deleted prematurely
    ++it_group->refererrs;

    /// Wait a notification until we will be the only in the group.
    it_group->cv.wait(lock, [&] () { return it_group == queue.begin(); });

    --it_group->refererrs;

    if (request_has_query_id)
    {
        try
        {
            if (type == Type::Read)
                all_read_locks.add(query_id);              /// SM2: may throw on insertion
                                                           ///      and is safe to roll back unconditionally
            const auto emplace_res =
                    owner_queries.emplace(query_id, 1);    /// SM3: may throw on insertion
            if (!emplace_res.second)
                ++emplace_res.first->second;               /// SM4: nothrow
        }
        catch (...)
        {
            /// Methods std::list<>::emplace_back() and std::unordered_map<>::emplace() provide strong exception safety
            /// We only need to roll back the changes to these objects: all_read_locks and the locking queue
            if (type == Type::Read)
                all_read_locks.remove(query_id);           /// Rollback(SM2): nothrow

            if (it_group->refererrs == 0)
            {
                const auto next = queue.erase(it_group);   /// Rollback(SM1): nothrow
                if (next != queue.end())
                    next->cv.notify_all();
            }

            throw;
        }
    }

    lock_holder->bind_with(shared_from_this(), it_group);  /// SM: nothrow

    finalize_metrics();
    return lock_holder;
}


/** The sequence points of acquiring lock's ownership by an instance of LockHolderImpl:
  *   1. all_read_locks is updated
  *   2. owner_queries is updated
  *   3. request group is updated by LockHolderImpl which in turn becomes "bound"
  *
  * If by the time when destructor of LockHolderImpl is called the instance has been "bound",
  * it is guaranteed that all three steps have been executed successfully and the resulting state is consistent.
  * With the mutex locked the order of steps to restore the lock's state can be arbitrary
  *
  * We do not employ try-catch: if something bad happens, there is nothing we can do =(
  */
RWLockImpl::LockHolderImpl::~LockHolderImpl()
{
    if (!bound || parent == nullptr)
        return;

    std::lock_guard lock(parent->mutex);

    /// The associated group must exist (and be the beginning of the queue?)
    if (parent->queue.empty() || it_group != parent->queue.begin())
        return;

    /// If query_id is not empty it must be listed in parent->owner_queries
    if (query_id != RWLockImpl::NO_QUERY)
    {
        const auto owner_it = parent->owner_queries.find(query_id);
        if (owner_it != parent->owner_queries.end())
        {
            if (--owner_it->second == 0)                  /// SM: nothrow
                parent->owner_queries.erase(owner_it);    /// SM: nothrow

            if (lock_type == RWLockImpl::Read)
                all_read_locks.remove(query_id);          /// SM: nothrow
        }
    }

    /// If we are the last remaining referrer, remove the group and notify the next group
    if (--it_group->refererrs == 0)                       /// SM: nothrow
    {
        const auto next = parent->queue.erase(it_group);  /// SM: nothrow
        if (next != parent->queue.end())
            next->cv.notify_all();
    }
}

}
