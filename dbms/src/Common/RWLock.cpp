#include "RWLock.h"
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <common/logger_useful.h>


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


class RWLockImpl::LockHolderImpl
{
    RWLock parent;
    GroupsContainer::iterator it_group;
    ClientsContainer::iterator it_client;
    QueryIdToHolder::key_type query_id;
    CurrentMetrics::Increment active_client_increment;

    LockHolderImpl(RWLock && parent, GroupsContainer::iterator it_group, ClientsContainer::iterator it_client);

public:

    LockHolderImpl(const LockHolderImpl & other) = delete;

    ~LockHolderImpl();

    friend class RWLockImpl;
};


RWLockImpl::LockHolder RWLockImpl::getLock(RWLockImpl::Type type, const String & query_id)
{
    Logger * log = &Logger::get("RWLock");

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

    GroupsContainer::iterator it_group;
    ClientsContainer::iterator it_client;

    std::unique_lock lock(mutex);

    LOG_TRACE(log, "Get lock " << static_cast<void*>(this) << " " << (type == Read ? "Read" : "Write") << " for query " << query_id);

    /// Check if the same query is acquiring previously acquired lock
    LockHolder existing_holder_ptr;

    auto it_query = query_id_to_holder.end();
    if (query_id != RWLockImpl::NO_QUERY)
    {
        it_query = query_id_to_holder.find(query_id);
        if (it_query != query_id_to_holder.end())
            existing_holder_ptr = it_query->second.lock();
    }

    if (existing_holder_ptr)
    {
        /// XXX: it means we can't upgrade lock from read to write - with proper waiting!
        if (type != Read || existing_holder_ptr->it_group->type != Read)
            throw Exception("Attempt to acquire exclusive lock recursively", ErrorCodes::LOGICAL_ERROR);

        LOG_TRACE(log, "Returning existing lock " << static_cast<void*>(this) << " " << (type == Read ? "Read" : "Write") << " for query " << query_id);
        return existing_holder_ptr;
    }

    if (type == Type::Write || queue.empty() || queue.back().type == Type::Write)
    {
        /// Create new group of clients
        it_group = queue.emplace(queue.end(), type);
    }
    else
    {
        /// Will append myself to last group
        it_group = std::prev(queue.end());
    }

    /// Append myself to the end of chosen group
    auto & clients = it_group->clients;
    try
    {
        it_client = clients.emplace(clients.end(), type);
    }
    catch (...)
    {
        /// Remove group if it was the first client in the group and an error occurred
        if (clients.empty())
            queue.erase(it_group);
        throw;
    }

    LOG_TRACE(log, "Creating new lock " << static_cast<void*>(this) << " " << (type == Read ? "Read" : "Write") << " for query " << query_id);
    LockHolder res(new LockHolderImpl(shared_from_this(), it_group, it_client));

    /// Insert myself (weak_ptr to the holder) to queries set to implement recursive lock
    if (query_id != RWLockImpl::NO_QUERY)
        query_id_to_holder.emplace(query_id, res);
    res->query_id = query_id;

    /// We are first, we should not wait anything
    /// If we are not the first client in the group, a notification could be already sent
    if (it_group == queue.begin())
    {
        finalize_metrics();
        return res;
    }

    /// Wait a notification
    LOG_TRACE(log, "Waiting lock " << static_cast<void*>(this) << " " << (type == Read ? "Read" : "Write") << " for query " << query_id);
    it_group->cv.wait(lock, [&] () { return it_group == queue.begin(); });

    finalize_metrics();
    return res;
}


RWLockImpl::LockHolderImpl::~LockHolderImpl()
{
    std::unique_lock lock(parent->mutex);

    LOG_TRACE(&Logger::get("RWLock"), "Releasing lock " << static_cast<void*>(parent.get()) << " for query " << query_id);

    /// Remove weak_ptrs to the holder, since there are no owners of the current lock
    parent->query_id_to_holder.erase(query_id);

    /// Removes myself from client list of our group
    it_group->clients.erase(it_client);

    /// Remove the group if we were the last client and notify the next group
    if (it_group->clients.empty())
    {
        auto & parent_queue = parent->queue;
        parent_queue.erase(it_group);

        if (!parent_queue.empty())
            parent_queue.front().cv.notify_all();
    }
}


RWLockImpl::LockHolderImpl::LockHolderImpl(RWLock && parent, RWLockImpl::GroupsContainer::iterator it_group,
                                             RWLockImpl::ClientsContainer::iterator it_client)
    : parent{std::move(parent)}, it_group{it_group}, it_client{it_client},
      active_client_increment{(*it_client == RWLockImpl::Read) ? CurrentMetrics::RWLockActiveReaders
                                                               : CurrentMetrics::RWLockActiveWriters}
{}

}
