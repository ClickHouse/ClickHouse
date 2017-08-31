#include <boost/core/noncopyable.hpp>
#include "RWLockFIFO.h"

namespace DB
{


RWLockFIFO::LockHandler RWLockFIFO::getLock(RWLockFIFO::Type type, RWLockFIFO::Client client)
{
    GroupsContainer::iterator it_group;
    ClientsContainer::iterator it_client;

    std::unique_lock<std::mutex> lock(mutex);


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
        it_client = clients.emplace(clients.end(), std::move(client));
    }
    catch (...)
    {
        /// Remove group if it was the first client in the group and an error occurred
        if (clients.empty())
            queue.erase(it_group);
        throw;
    }

    LockHandler res = std::make_unique<LockHandlerImpl>(shared_from_this(), it_group, it_client);

    /// We are first, we should not wait anything
    /// If we are not the first client in the group, a notification could be already sent
    if (it_group == queue.begin())
        return res;

    /// Wait a notification
    it_group->cv.wait(lock, [&it_group] () { return it_group->awakened; } );
    return res;
}


void RWLockFIFO::LockHandlerImpl::unlock()
{
    std::unique_lock<std::mutex> lock(parent->mutex);

    auto & clients = it_group->clients;
    clients.erase(it_client);

    if (clients.empty())
    {
        auto & queue = parent->queue;
        queue.erase(it_group);

        if (!queue.empty())
        {
            queue.front().awakened = true;
            queue.front().cv.notify_all();
        }
    }
}


RWLockFIFO::LockHandlerImpl::~LockHandlerImpl()
{
    unlock();
}


}
