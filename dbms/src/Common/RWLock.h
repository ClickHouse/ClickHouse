#pragma once

#include <Common/Exception.h>

#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>


namespace DB
{

/// Implements shared lock with FIFO service
/// Can be acquired recursively (several calls for the same query or the same OS thread) in Read mode
///
/// NOTE: it is important to allow acquiring the same lock in Read mode without waiting if it is already
/// acquired by another thread of the same query. Otherwise the following deadlock is possible:
/// - SELECT thread 1 locks in the Read mode
/// - ALTER tries to lock in the Write mode (waits for SELECT thread 1)
/// - SELECT thread 2 tries to lock in the Read mode (waits for ALTER)

/**
  *  Lock object's invariants ("query_id-wise" fairness):
  *   - If query_id is not specified for locking request, use thread_id for lock aggregation
  *   - Lock is reenterable (a thread requests a lock while it is already holding the lock) if the request is compatible (R -> R)
  *   - Incompatible locking requests from thread raise EDEADLOCK (R -> W, W -> R, W -> W)
  *   - Any given query_id can be present inside of only one group in the locking queue
  *   - Every request in the locking queue comes from a different thread
  *   - All locking requests are grouped into RequestGroups by query_id
  *   - Only threads that are not in the waiting queue are able to issue new locking requests
  *   - Lock is considered "free" when the list of owners and locking queue are both empty
  */
class RWLockImpl
{
private:
    enum class State : unsigned char;

public:
    /// Use as query_id to acquire a lock outside the query context.
    inline static const String NO_QUERY = String();

    using LockId = uint64_t;

    ///  Call returns with an id of a lock taken in Exclusive/Shared mode (use lock_id when unlocking)
    LockId lock(const String & query_id = NO_QUERY);
    LockId lock_shared(const String & query_id = NO_QUERY);

    ///  Use the appropriate lock_id when unlocking!
    void unlock(LockId lock_id);
    void unlock_shared(LockId lock_id);

private:
    LockId lock_impl(State, const String &);
    void unlock_impl(LockId);

private:
    enum class State : unsigned char
    {
        Idle,
        Shared,
        Exclusive,
    };

    enum class Mode
    {
        Shared,
        Exclusive,
    };

    struct ClientInfo
    {
        String query_id;
        std::thread::id thread_id;
    };

    // Locking requests in read mode can be collected into one group depending on locking modes compatibility
    struct RequestGroup
    {
        const Mode locking_mode;
        size_t waiters;

        std::condition_variable cv; /// all threads wait on group common condvar

        explicit RequestGroup(const Mode mode) : locking_mode{mode}, waiters{0} {}
    };

    using LockInfo = std::unordered_map<LockId, ClientInfo>;
    using RequestsQueue = std::list<RequestGroup>;

    /// LockId generator for issuing lock identifiers to successful locking requests
    LockId id_generator = 0;
    State lock_state = State::Idle;

    LockInfo active_locks;
    std::unordered_map<std::thread::id, size_t> lock_owner_threads;
    std::unordered_map<String, size_t> lock_owner_queries;

    /// A queue - for fairness guarantees
    RequestsQueue pending_requests_queue;
    /// Queries that are enqueued for locking - for aggregating locking requests in Shared mode
    std::unordered_map<String, RequestsQueue::iterator> enqueued_queries;

    mutable std::mutex mutex;
};


template <bool Exclusive>
class RWLockHolderBase
{
public:
    RWLockHolderBase() noexcept
        : rw_lock_impl{nullptr}, owns{false}, lock_id{0ull}
    {}
    explicit RWLockHolderBase(RWLockImpl & _rw_lock_impl)
        : rw_lock_impl{&_rw_lock_impl}, owns{true}
    {
        lock_id = Exclusive ? rw_lock_impl->lock() : rw_lock_impl->lock_shared();
    }
    RWLockHolderBase(RWLockImpl & _rw_lock_impl, const String & query_id)
        : rw_lock_impl{&_rw_lock_impl}, owns{true}
    {
        lock_id = Exclusive ? rw_lock_impl->lock(query_id) : rw_lock_impl->lock_shared(query_id);
    }

private:
    RWLockHolderBase(const RWLockHolderBase &) = delete;
    RWLockHolderBase& operator=(const RWLockHolderBase &) = delete;

public:
    RWLockHolderBase(RWLockHolderBase && other) noexcept
        : rw_lock_impl{other.rw_lock_impl},
          owns{other.owns},
          lock_id{other.lock_id}
    {
        other.rw_lock_impl = nullptr;
        other.owns = false;
        other.lock_id = 0ull;
    }

    RWLockHolderBase& operator=(RWLockHolderBase && other)
    {
        RWLockHolderBase::unlock_impl();

        rw_lock_impl = other.rw_lock_impl;
        owns = other.owns;
        lock_id = other.lock_id;

        other.rw_lock_impl = nullptr;
        other.owns = false;
        other.lock_id = 0ull;

        return *this;
    }

    void release();

    bool owns_lock() const noexcept { return owns; }
    explicit operator bool() const noexcept { return owns; }

    ~RWLockHolderBase() { RWLockHolderBase::unlock_impl(); }

private:
    void unlock_impl()
    {
        if (owns)
            Exclusive ? rw_lock_impl->unlock(lock_id) : rw_lock_impl->unlock_shared(lock_id);
    }

private:
    RWLockImpl * rw_lock_impl;
    bool owns;
    RWLockImpl::LockId lock_id;
};

using SharedLockHolder = RWLockHolderBase<false>;
using ExclusiveLockHolder = RWLockHolderBase<true>;

using SharedLockHolderPtr = std::shared_ptr<SharedLockHolder>;
using ExclusiveLockHolderPtr = std::shared_ptr<ExclusiveLockHolder>;


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <bool Exclusive>
void RWLockHolderBase<Exclusive>::release()
{
    if (!rw_lock_impl)
        throw Exception("RWLockHolderBase::release(): references null mutex", ErrorCodes::LOGICAL_ERROR);
    if (!owns)
        throw Exception("RWLockHolderBase::release(): not locked", ErrorCodes::LOGICAL_ERROR);
    RWLockHolderBase::unlock_impl();
    rw_lock_impl = nullptr;
    owns = false;
    lock_id = 0ull;
}

}
