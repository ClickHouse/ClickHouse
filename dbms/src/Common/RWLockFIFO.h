#pragma once
#include <boost/core/noncopyable.hpp>
#include <list>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <map>
#include <string>


namespace DB
{

class RWLockFIFO;
using RWLockFIFOPtr = std::shared_ptr<RWLockFIFO>;


/// Implements shared lock with FIFO service
/// You could call it recursively (several calls from the same thread) in Read mode
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

        bool isStarted() { return start_time != 0; }

        /// TODO: delete extra info below if there is no need fot it already.
        std::string info;
        int thread_number = 0;
        std::time_t enqueue_time = 0;
        std::time_t start_time = 0;
        Type type;
    };


    /// Just use LockHandler::reset() to release the lock
    class LockHandlerImpl;
    friend class LockHandlerImpl;
    using LockHandler = std::shared_ptr<LockHandlerImpl>;


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
    using ThreadToHandler = std::map<std::thread::id, std::weak_ptr<LockHandlerImpl>>;

    /// Group of clients that should be executed concurrently
    /// i.e. a group could contain several readers, but only one writer
    struct Group
    {
        const Type type;
        ClientsContainer clients;

        std::condition_variable cv; /// all clients of the group wait group condvar

        explicit Group(Type type) : type{type} {}
    };

    mutable std::mutex mutex;
    GroupsContainer queue;
    ThreadToHandler thread_to_handler;
};


}
