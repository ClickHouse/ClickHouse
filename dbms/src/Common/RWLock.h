#pragma once
#include <Core/Types.h>
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
/// Can be acquired recursively (several calls for the same query or the same OS thread) in Read mode
class RWLockImpl : public std::enable_shared_from_this<RWLockImpl>
{
public:
    enum Type
    {
        Read,
        Write,
    };

    static RWLock create() { return RWLock(new RWLockImpl); }

    /// Just use LockHolder::reset() to release the lock
    class LockHolderImpl;
    friend class LockHolderImpl;
    using LockHolder = std::shared_ptr<LockHolderImpl>;


    /// Waits in the queue and returns appropriate lock
    /// Empty query_id means the lock is acquired out of the query context (e.g. in a background thread).
    LockHolder getLock(Type type, const String & query_id = String());

private:
    RWLockImpl() = default;

    struct Group;
    using GroupsContainer = std::list<Group>;
    using ClientsContainer = std::list<Type>;
    using ThreadToHolder = std::map<std::thread::id, std::weak_ptr<LockHolderImpl>>;
    using QueryIdToHolder = std::map<String, std::weak_ptr<LockHolderImpl>>;

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
    ThreadToHolder thread_to_holder;
    QueryIdToHolder query_id_to_holder;
};


}
