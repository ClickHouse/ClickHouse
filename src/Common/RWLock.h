#pragma once

#include <Core/Types.h>

#include <chrono>
#include <list>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <map>
#include <string>
#include <unordered_map>


namespace DB
{

class RWLockImpl;
using RWLock = std::shared_ptr<RWLockImpl>;


/// Implements Readers-Writers locking algorithm that serves requests in "Phase Fair" order.
/// (Phase Fair RWLock as suggested in https://www.cs.unc.edu/~anderson/papers/rtsj10-for-web.pdf)
/// It is used for synchronizing access to various objects on query level (i.e. Storages).
///
/// In general, ClickHouse processes queries by multiple threads of execution in parallel.
/// As opposed to the standard OS synchronization primitives (mutexes), this implementation allows
/// unlock() to be called by a thread other than the one, that called lock().
/// It is also possible to acquire RWLock in Read mode without waiting (FastPath) by multiple threads,
/// that execute the same query (share the same query_id).
///
/// NOTE: it is important to allow acquiring the same lock in Read mode without waiting if it is already
/// acquired by another thread of the same query. Otherwise the following deadlock is possible:
/// - SELECT thread 1 locks in the Read mode
/// - ALTER tries to lock in the Write mode (waits for SELECT thread 1)
/// - SELECT thread 2 tries to lock in the Read mode (waits for ALTER)
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

    /// Empty query_id means the lock is acquired from outside of query context (e.g. in a background thread).
    LockHolder getLock(Type type, const String & query_id,
                       const std::chrono::milliseconds & lock_timeout_ms = std::chrono::milliseconds(0));

    /// Use as query_id to acquire a lock outside the query context.
    inline static const String NO_QUERY = String();
    inline static const auto default_locking_timeout_ms = std::chrono::milliseconds(120000);

private:
    /// Group of locking requests that should be granted simultaneously
    /// i.e. one or several readers or a single writer
    struct Group
    {
        const Type type;
        size_t requests;

        std::condition_variable cv; /// all locking requests of the group wait on this condvar

        explicit Group(Type type_) : type{type_}, requests{0} {}
    };

    using GroupsContainer = std::list<Group>;
    using OwnerQueryIds = std::unordered_map<String, size_t>;

private:
    mutable std::mutex internal_state_mtx;

    GroupsContainer readers_queue;
    GroupsContainer writers_queue;
    GroupsContainer::iterator rdlock_owner{readers_queue.end()};  /// equals to readers_queue.begin() in read phase
                                                                  /// or readers_queue.end() otherwise
    GroupsContainer::iterator wrlock_owner{writers_queue.end()};  /// equals to writers_queue.begin() in write phase
                                                                  /// or writers_queue.end() otherwise
    OwnerQueryIds owner_queries;

private:
    RWLockImpl() = default;
    void unlock(GroupsContainer::iterator group_it, const String & query_id) noexcept;
    void dropOwnerGroupAndPassOwnership(GroupsContainer::iterator group_it) noexcept;
};
}
