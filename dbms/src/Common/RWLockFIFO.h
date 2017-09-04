#pragma once
#include <boost/core/noncopyable.hpp>
#include <list>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <string>


namespace DB
{

class RWLockFIFO;
using RWLockFIFOPtr = std::shared_ptr<RWLockFIFO>;


/// Implements shared lock with FIFO service
/// It does not work as recursive mutex, so a deadlock will occur if you try to acquire 2 locks in the same thread
class RWLockFIFO : public std::enable_shared_from_this<RWLockFIFO>
{
public:

    static RWLockFIFOPtr create()
    {
        return RWLockFIFOPtr(new RWLockFIFO);
    }

    enum Type
    {
        Read,
        Write
    };

    /// Client is that who wants to acquire the lock.
    struct Client
    {
        explicit Client(const std::string & info = {}) : info{info} {}

        std::string info;
        int thread_number = 0;
        std::time_t enqueue_time = 0;
        std::time_t start_time = 0;
        Type type;
    };

    class LockHandlerImpl;
    using LockHandler = std::unique_ptr<LockHandlerImpl>;

    /// Waits in the queue and returns appropriate lock
    LockHandler getLock(Type type, Client client = Client{});

    LockHandler getLock(Type type, const std::string & who)
    {
        return getLock(type, Client(who));
    }

    using Clients = std::vector<Client>;

    /// Returns list of executing and waiting clients
    Clients getClientsInTheQueue() const;

private:

    RWLockFIFO() = default;

    struct Group;
    using GroupsContainer = std::list<Group>;
    using ClientsContainer = std::list<Client>;

    /// Group of clients that should be executed concurrently
    /// i.e. a group could contain several readers, but only one writer
    struct Group
    {
        const Type type;
        ClientsContainer clients;

        std::condition_variable cv; /// all clients of the group wait group condvar

        explicit Group(Type type) : type{type} {}
    };

public:

    class LockHandlerImpl
    {
        RWLockFIFOPtr parent;
        GroupsContainer::iterator it_group;
        ClientsContainer::iterator it_client;

    public:
        LockHandlerImpl(RWLockFIFOPtr && parent, GroupsContainer::iterator it_group, ClientsContainer::iterator it_client)
                : parent{std::move(parent)}, it_group{it_group}, it_client{it_client} {}

        LockHandlerImpl(const LockHandlerImpl & other) = delete;

        /// Unlocks acquired lock
        void unlock();

        ~LockHandlerImpl();

        friend class RWLockFIFO;
    };

private:

    mutable std::mutex mutex;
    GroupsContainer queue;
};


}
