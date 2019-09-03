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
}


/** Locking scenarios are somewhat different for locking requests in Shared and eXclusive modes:
 *
 *  Exclusive mode locking request:
 *  1. Check if this thread already holds the lock - Ensure that we would not deadlock
 *  2. If the lock is not in state "Idle":
 *     - Add*) itself to the queue
 *     - Block until the current group is to become the lock owner
 *  3. Take ownership: Add self to the lock owners list and return with a new lock_id
 *
 *  Shared mode locking request:
 *  1. This thread cannot progress and the request has to wait in the queue if:
 *     - The lock is in state "Exclusive", or
 *     - The lock is taken in "Shared" mode and this thread and query_id are not on the owners list
 *     - Add*) itself to the queue
 *     - Block until the current group is to become the lock owner
 *  2. Take ownership: Add self to the lock owners list and return with a new lock_id
 *
 *  *) Adding is possible to the group which is assosiated with
 *     the same query_id or to the back given that it is of compatible mode
 */
RWLockImpl::LockId RWLockImpl::lock_impl(State target_state, const String & query_id)
{
    /// TODO(akazz): IMPORTANT! Exception safety: If lock* throws the lock must not change its state!!!
    const std::thread::id this_thread_id = std::this_thread::get_id();

    // TODO: Make use of type optional here?
    const bool request_with_query_id = (query_id != NO_QUERY);

    std::unique_lock<std::mutex> lock(mutex);

    switch (target_state)
    {
    case State::Exclusive:
        if (lock_owner_threads.find(this_thread_id) != lock_owner_threads.cend())
            throw Exception("Acquiring RWLock in Exclusive mode: the thread already owns this lock in Exclusive mode, "
                            "it would deadlock on itself", ErrorCodes::LOGICAL_ERROR);

        if (lock_state != State::Idle)
        {
            const auto my_request_group_it = pending_requests_queue.emplace(pending_requests_queue.cend(), Mode::Exclusive);
            while (!active_locks.empty() || my_request_group_it != pending_requests_queue.cbegin())
                my_request_group_it->cv.wait(lock);
            pending_requests_queue.pop_front();
        }

        break;

    case State::Shared:
        if (lock_state == State::Exclusive ||
            (lock_state == State::Shared &&
             lock_owner_threads.find(this_thread_id) == lock_owner_threads.cend() &&
             (!request_with_query_id || lock_owner_queries.find(query_id) == lock_owner_queries.cend())))
        {
            RequestsQueue::iterator my_request_group_it;

            if (request_with_query_id && enqueued_queries.count(query_id) > 0)
            {
                my_request_group_it = enqueued_queries[query_id];
            }
            else if (!pending_requests_queue.empty() && pending_requests_queue.back().locking_mode == Mode::Shared)
            {
                my_request_group_it = std::prev(pending_requests_queue.end());
            }
            else
            {
                my_request_group_it = pending_requests_queue.emplace(pending_requests_queue.cend(), Mode::Shared);
                if (request_with_query_id)
                    enqueued_queries.emplace(query_id, my_request_group_it);
            }

            ++my_request_group_it->waiters;

            while (!active_locks.empty() || my_request_group_it != pending_requests_queue.cbegin())
                my_request_group_it->cv.wait(lock);

            --my_request_group_it->waiters;
            if (my_request_group_it->waiters == 0)
            {
                if (request_with_query_id)
                    enqueued_queries.erase(query_id);
                pending_requests_queue.pop_front();
            }
        }

        break;

    default:
        throw Exception("Illegal locking mode", ErrorCodes::LOGICAL_ERROR);
    }

    ++lock_owner_threads[this_thread_id];
    if (request_with_query_id)
        ++lock_owner_queries[query_id];
    active_locks.emplace(id_generator, ClientInfo{query_id, this_thread_id});

    lock_state = target_state;

    return id_generator++;
}


void RWLockImpl::unlock_impl(LockId lock_id)
{
    std::lock_guard lock(mutex);

    const auto client_info_it = active_locks.find(lock_id);
    if (client_info_it == active_locks.cend())
        throw Exception(
                "RWLockImpl::unlock_impl(): the specified lock_id cannot be found (lock has already been released?)",
                ErrorCodes::LOGICAL_ERROR);

    const auto & client_info = client_info_it->second;

    if (client_info.thread_id != std::this_thread::get_id())
        throw Exception("RWLockImpl::unlock_impl(): attempting to unlock by a different thread", ErrorCodes::LOGICAL_ERROR);

    if (client_info.query_id != NO_QUERY)
    {
        if (--lock_owner_queries.at(client_info.query_id) == 0)
            lock_owner_queries.erase(client_info.query_id);
    }
    --lock_owner_threads.at(client_info.thread_id);
    if (lock_owner_threads.at(client_info.thread_id) == 0)
        lock_owner_threads.erase(client_info.thread_id);
    active_locks.erase(lock_id);

    if (active_locks.empty())
    {
        if (pending_requests_queue.empty())
            lock_state = State::Idle;
        else
            /// We do not need to explicitly adjust the lock state here - the notified group will take care of it upon wakeup
            pending_requests_queue.begin()->cv.notify_all();
    }
    else if (lock_state == State::Exclusive)
        throw Exception("Should never happen", ErrorCodes::LOGICAL_ERROR);
}


RWLockImpl::LockId RWLockImpl::lock(const String & query_id)
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    CurrentMetrics::Increment waiting_client_increment(CurrentMetrics::RWLockWaitingWriters);

    const auto lock_id = lock_impl(State::Exclusive, query_id);

    CurrentMetrics::add(CurrentMetrics::RWLockActiveWriters);

    ProfileEvents::increment(ProfileEvents::RWLockAcquiredWriteLocks);
    ProfileEvents::increment(ProfileEvents::RWLockWritersWaitMilliseconds, watch.elapsedMilliseconds());

    return lock_id;
}


void RWLockImpl::unlock(LockId lock_id)
{
    unlock_impl(lock_id);

    CurrentMetrics::sub(CurrentMetrics::RWLockActiveWriters);
}


RWLockImpl::LockId RWLockImpl::lock_shared(const String & query_id)
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    CurrentMetrics::Increment waiting_client_increment(CurrentMetrics::RWLockWaitingReaders);

    const auto lock_id = lock_impl(State::Shared, query_id);

    CurrentMetrics::add(CurrentMetrics::RWLockActiveReaders);

    ProfileEvents::increment(ProfileEvents::RWLockAcquiredReadLocks);
    ProfileEvents::increment(ProfileEvents::RWLockReadersWaitMilliseconds, watch.elapsedMilliseconds());

    return lock_id;
}


void RWLockImpl::unlock_shared(LockId lock_id)
{
    unlock_impl(lock_id);

    CurrentMetrics::sub(CurrentMetrics::RWLockActiveReaders);
}

}
