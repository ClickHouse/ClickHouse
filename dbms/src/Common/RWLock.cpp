#include "RWLock.h"
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <cassert>


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


class RWLockImpl::LockHolderImpl
{
    RWLock parent;
    GroupsContainer::iterator it_group;
    QueryIdToHolder::key_type query_id;
    CurrentMetrics::Increment active_client_increment;

    LockHolderImpl(RWLock && parent, GroupsContainer::iterator it_group);

public:

    LockHolderImpl(const LockHolderImpl & other) = delete;

    ~LockHolderImpl();

    friend class RWLockImpl;
};


namespace
{
    /// Global information about all read locks that query has. It is needed to avoid some type of deadlocks.

    class QueryLockInfo
    {
    private:
        std::mutex mutex;
        std::map<std::string, size_t> queries;

    public:
        void add(const String & query_id)
        {
            std::lock_guard lock(mutex);
            ++queries[query_id];
        }

        void remove(const String & query_id)
        {
            std::lock_guard lock(mutex);
            auto it = queries.find(query_id);
            assert(it != queries.end());
            if (--it->second == 0)
                queries.erase(it);
        }

        void check(const String & query_id)
        {
            std::lock_guard lock(mutex);
            if (queries.count(query_id))
                throw Exception("Possible deadlock avoided. Client should retry.", ErrorCodes::DEADLOCK_AVOIDED);
        }
    };

    QueryLockInfo all_read_locks;
}


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
    LockHolder res;

    std::unique_lock<std::mutex> lock(mutex);

    /// Check if the same query is acquiring previously acquired lock
    /// This FastPath tries to create a "light copy" of a lock without creating a separate LockHolderImpl
    if (request_has_query_id)
    {
        const auto it_query = query_id_to_holder.find(query_id);
        if (it_query != query_id_to_holder.end())
        {
            res = it_query->second.lock();
            if (res)
            {
                /// XXX: it means we can't upgrade lock from read to write - with proper waiting!
                if (type == Write)
                    throw Exception(
                            "RWLockImpl::getLock(): Cannot acquire exclusive lock while RWLock is already locked",
                            ErrorCodes::LOGICAL_ERROR);

                if (res->it_group->type == Write)
                    throw Exception(
                            "RWLockImpl::getLock(): RWLock is already locked in exclusive mode",
                            ErrorCodes::LOGICAL_ERROR);

                finalize_metrics();
                return res;
            }
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

    GroupsContainer::iterator it_group;

    /// A locking request is considered potentially dangerous if it needs to wait in the queue and
    /// its associated query_id already holds at least one Read lock. We raise an exception in such case
    if (type == Type::Write || queue.empty() || queue.back().type == Type::Write)
    {
        if (type == Type::Read && request_has_query_id && !queue.empty())
            all_read_locks.check(query_id);

        /// Create a new group of locking requests
        it_group = queue.emplace(queue.end(), type);
    }
    else
    {
        if (type == Type::Read && request_has_query_id && queue.size() > 1)
            all_read_locks.check(query_id);

        /// Will append myself to last group
        it_group = std::prev(queue.end());
    }
    ++it_group->referers;
    res.reset(new LockHolderImpl(shared_from_this(), it_group));

    /// Wait a notification until we will be the only in the group.
    it_group->cv.wait(lock, [&] () { return it_group == queue.begin(); });

    /// Insert myself (weak_ptr to the holder) to queries set to implement recursive lock
    if (request_has_query_id)
    {
        query_id_to_holder.emplace(query_id, res);

        if (type == Type::Read)
            all_read_locks.add(query_id);
        res->query_id = query_id;
    }

    finalize_metrics();
    return res;
}


RWLockImpl::LockHolderImpl::~LockHolderImpl()
{
    std::lock_guard lock(parent->mutex);

    if (query_id != RWLockImpl::NO_QUERY)
    {
        /// Since every access to query_id_to_holder is syncronized, expired() == true means
        /// that the current lock holder being destroyed is the last remaining to be associated with query_id
        /// (query_id_to_holder is only updated in PointA)
        /// so we can safely erase query_id, otherwise there still are other lock holders for this query_id
        ///
        /// XXX: There might be other destructors as well running concurrently that might have already erased this query_id
        const auto it_query_id = parent->query_id_to_holder.find(query_id);
        if (it_query_id != parent->query_id_to_holder.end())
        {
            if (it_query_id->second.expired())
                parent->query_id_to_holder.erase(it_query_id);
        }

        if (it_group->type == RWLockImpl::Read)
            all_read_locks.remove(query_id);
    }

    /// Remove the group if we were the last referer and notify the next group
    if (--it_group->referers == 0)
    {
        auto & parent_queue = parent->queue;
        parent_queue.erase(it_group);

        if (!parent_queue.empty())
            parent_queue.front().cv.notify_all();
    }
}


RWLockImpl::LockHolderImpl::LockHolderImpl(RWLock && parent_, RWLockImpl::GroupsContainer::iterator it_group_)
    : parent{std::move(parent_)}, it_group{it_group_},
      active_client_increment{(it_group_->type == RWLockImpl::Read) ? CurrentMetrics::RWLockActiveReaders
                                                               : CurrentMetrics::RWLockActiveWriters}
{
}

}
