#pragma once
#include <list>
#include <mutex>
#include <condition_variable>
#include <Common/Exception.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


struct RWLockFIFO;
using RWLockFIFOPtr = std::shared_ptr<RWLockFIFO>;


/// Implements shared lock with FIFO service
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
        explicit Client(const std::string & info = "Anonymous client") : info{info} {}
        std::string info;
    };

    class LockHandlerImpl;
    using LockHandler = std::unique_ptr<LockHandlerImpl>;

    /// Waits in the queue and returns appropriate lock
    LockHandler getLock(Type type, Client client);

    LockHandler getLock(Type type, const std::string & who)
    {
        return getLock(type, Client(who));
    }

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
        bool awakened{false}; /// just only to handle spurious wake ups

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

        void unlock();

        ~LockHandlerImpl();

        friend class RWLockFIFO;
    };

private:

    std::mutex mutex;
    GroupsContainer queue;
};


}
