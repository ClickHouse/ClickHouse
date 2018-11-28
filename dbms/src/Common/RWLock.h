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

class RWLockImpl;
using RWLock = std::shared_ptr<RWLockImpl>;


/// Implements shared lock with FIFO service
/// Can be acquired recursively (several calls from the same thread) in Read mode
class RWLockImpl : public std::enable_shared_from_this<RWLockImpl>
{
public:
    enum Type
    {
        Read,
        Write,
    };

    static RWLock create() { return RWLock(new RWLockImpl); }

    /// Just use LockHandler::reset() to release the lock
    class LockHandlerImpl;
    friend class LockHandlerImpl;
    using LockHandler = std::shared_ptr<LockHandlerImpl>;


    /// Waits in the queue and returns appropriate lock
    LockHandler getLock(Type type);

private:
    RWLockImpl() = default;

    struct Group;
    using GroupsContainer = std::list<Group>;
    using ClientsContainer = std::list<Type>;
    using ThreadToHandler = std::map<std::thread::id, std::weak_ptr<LockHandlerImpl>>;

    /// Group of clients that should be executed concurrently
    /// i.e. a group could contain several readers, but only one writer
    struct Group
    {
        // FIXME: there is only redundant |type| information inside |clients|.
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
