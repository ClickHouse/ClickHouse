#pragma once

#include <base/types.h>

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
///
/// NOTE: it is dangerous to acquire lock with NO_QUERY, because FastPath doesn't
/// exist for this case and deadlock, described in previous note,
/// may occur in case of recursive locking.
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
                       const std::chrono::milliseconds & lock_timeout_ms = std::chrono::milliseconds(0), bool throw_in_fast_path = true);

    /// Use as query_id to acquire a lock outside the query context.
    inline static const String NO_QUERY = String();
    inline static const auto default_locking_timeout_ms = std::chrono::milliseconds(120000);

    /// Returns all query_id owning locks (both read and write) right now.
    /// !! This function are for debugging and logging purposes only, DO NOT use them for synchronization!
    std::unordered_map<String, size_t> getOwnerQueryIds() const;
    String getOwnerQueryIdsDescription() const;

private:
    /// Group of locking requests that should be granted simultaneously
    /// i.e. one or several readers or a single writer
    struct Group
    {
        const Type type;
        size_t requests = 0;

        bool ownership = false; /// whether this group got ownership? (that means `cv` is notified and the locking requests should stop waiting)
        std::condition_variable cv; /// all locking requests of the group wait on this condvar

        explicit Group(Type type_) : type{type_} {}
    };

    using GroupsContainer = std::list<Group>;
    using OwnerQueryIds = std::unordered_map<String /* query_id */, size_t /* num_owners */>;

    mutable std::mutex internal_state_mtx;

    GroupsContainer readers_queue;
    GroupsContainer writers_queue;
    GroupsContainer::iterator rdlock_owner{readers_queue.end()};  /// last group with ownership in readers_queue in read phase
                                                                  /// or readers_queue.end() in writer phase
    GroupsContainer::iterator wrlock_owner{writers_queue.end()};  /// equals to writers_queue.begin() in write phase
                                                                  /// or writers_queue.end() in read phase
    OwnerQueryIds owner_queries;

    RWLockImpl() = default;
    void unlock(GroupsContainer::iterator group_it, const String & query_id) noexcept;
    void dropOwnerGroupAndPassOwnership(GroupsContainer::iterator group_it) noexcept;
    void grantOwnership(GroupsContainer::iterator group_it) noexcept;
    void grantOwnershipToAllReaders() noexcept;
};
}
