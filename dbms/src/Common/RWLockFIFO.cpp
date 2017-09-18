#include "RWLockFIFO.h"
#include <Common/Exception.h>
#include <iostream>
#include <Poco/Ext/ThreadNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RWLockFIFO::LockHandler RWLockFIFO::getLock(RWLockFIFO::Type type, RWLockFIFO::Client client)
{
    GroupsContainer::iterator it_group;
    ClientsContainer::iterator it_client;

    auto this_thread_id = std::this_thread::get_id();

    std::unique_lock<std::mutex> lock(mutex);

    /// Check if the same thread is acquiring previously acquired lock
    auto it_handler = thread_to_handler.find(this_thread_id);
    if (it_handler != thread_to_handler.end())
    {
        auto handler_ptr = it_handler->second.lock();

        if (!handler_ptr)
            throw Exception("Lock handler cannot be nullptr. This is a bug", ErrorCodes::LOGICAL_ERROR);

        if (type != Read || handler_ptr->it_group->type != Read)
            throw Exception("Attempt to acquire exclusive lock recursively", ErrorCodes::LOGICAL_ERROR);

        handler_ptr->it_client->info += "; " + client.info;

        return  handler_ptr;
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
        it_client = clients.emplace(clients.end(), std::move(client));
    }
    catch (...)
    {
        /// Remove group if it was the first client in the group and an error occurred
        if (clients.empty())
            queue.erase(it_group);
        throw;
    }

    it_client->thread_number = Poco::ThreadNumber::get();
    it_client->enqueue_time = time(nullptr);
    it_client->type = type;

    LockHandler res(new LockHandlerImpl(shared_from_this(), it_group, it_client));

    /// Insert myself (weak_ptr to the handler) to threads set to implement recursive lock
    it_handler = thread_to_handler.emplace(this_thread_id, res).first;
    res->it_handler = it_handler;

    /// We are first, we should not wait anything
    /// If we are not the first client in the group, a notification could be already sent
    if (it_group == queue.begin())
    {
        it_client->start_time = it_client->enqueue_time;
        return res;
    }

    /// Wait a notification
    it_group->cv.wait(lock, [&] () { return it_group == queue.begin(); } );

    it_client->start_time = time(nullptr);
    return res;
}


RWLockFIFO::Clients RWLockFIFO::getClientsInTheQueue() const
{
    std::unique_lock<std::mutex> lock(mutex);

    Clients res;
    for (const auto & group : queue)
    {
        for (const auto & client : group.clients)
        {
            res.emplace_back(client);
        }
    }

    return res;
}


RWLockFIFO::LockHandlerImpl::~LockHandlerImpl()
{
    std::unique_lock<std::mutex> lock(parent->mutex);

    /// Remove weak_ptr to the handler, since there are no owners of the current lock
    parent->thread_to_handler.erase(it_handler);

    /// Removes myself from client list of our group
    it_group->clients.erase(it_client);

    /// Remove the group if we were the last client and notify the next group
    if (it_group->clients.empty())
    {
        auto & queue = parent->queue;
        queue.erase(it_group);

        if (!queue.empty())
            queue.front().cv.notify_all();
    }

    parent.reset();
}


RWLockFIFO::LockHandlerImpl::LockHandlerImpl(RWLockFIFOPtr && parent, RWLockFIFO::GroupsContainer::iterator it_group,
                                             RWLockFIFO::ClientsContainer::iterator it_client)
    : parent{std::move(parent)}, it_group{it_group}, it_client{it_client} {}

}
