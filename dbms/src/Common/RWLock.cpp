#include "RWLock.h"
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Poco/Ext/ThreadNumber.h>
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


class RWLockImpl::LockHandlerImpl
{
    RWLock parent;
    GroupsContainer::iterator it_group;
    ClientsContainer::iterator it_client;
    ThreadToHandler::iterator it_handler;
    CurrentMetrics::Increment active_client_increment;

    LockHandlerImpl(RWLock && parent, GroupsContainer::iterator it_group, ClientsContainer::iterator it_client);

public:

    LockHandlerImpl(const LockHandlerImpl & other) = delete;

    ~LockHandlerImpl();

    friend class RWLockImpl;
};


RWLockImpl::LockHandler RWLockImpl::getLock(RWLockImpl::Type type)
{
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

    auto this_thread_id = std::this_thread::get_id();
    GroupsContainer::iterator it_group;
    ClientsContainer::iterator it_client;

    std::unique_lock lock(mutex);

    /// Check if the same thread is acquiring previously acquired lock
    auto it_handler = thread_to_handler.find(this_thread_id);
    if (it_handler != thread_to_handler.end())
    {
        auto handler_ptr = it_handler->second.lock();

        /// Lock may be released in another thread, but not yet deleted inside |~LogHandlerImpl()|

        if (handler_ptr)
        {
            /// XXX: it means we can't upgrade lock from read to write - with proper waiting!
            if (type != Read || handler_ptr->it_group->type != Read)
                throw Exception("Attempt to acquire exclusive lock recursively", ErrorCodes::LOGICAL_ERROR);

            return handler_ptr;
        }
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

    LockHandler res(new LockHandlerImpl(shared_from_this(), it_group, it_client));

    /// Insert myself (weak_ptr to the handler) to threads set to implement recursive lock
    it_handler = thread_to_handler.emplace(this_thread_id, res).first;
    res->it_handler = it_handler;

    /// We are first, we should not wait anything
    /// If we are not the first client in the group, a notification could be already sent
    if (it_group == queue.begin())
    {
        finalize_metrics();
        return res;
    }

    /// Wait a notification
    it_group->cv.wait(lock, [&] () { return it_group == queue.begin(); });

    finalize_metrics();
    return res;
}


RWLockImpl::LockHandlerImpl::~LockHandlerImpl()
{
    std::unique_lock lock(parent->mutex);

    /// Remove weak_ptr to the handler, since there are no owners of the current lock
    parent->thread_to_handler.erase(it_handler);

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

    parent.reset();
}


RWLockImpl::LockHandlerImpl::LockHandlerImpl(RWLock && parent, RWLockImpl::GroupsContainer::iterator it_group,
                                             RWLockImpl::ClientsContainer::iterator it_client)
    : parent{std::move(parent)}, it_group{it_group}, it_client{it_client},
      active_client_increment{(*it_client == RWLockImpl::Read) ? CurrentMetrics::RWLockActiveReaders
                                                               : CurrentMetrics::RWLockActiveWriters}
{}

}
