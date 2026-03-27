#pragma once

#include <Common/SharedMutex.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <mutex>
#include <optional>
#include <shared_mutex>


namespace DB
{

namespace ProfiledLocks
{
    inline std::atomic<bool> profiled_locks_enabled{true};

    inline void setEnabled(bool enabled)
    {
        profiled_locks_enabled.store(enabled, std::memory_order_relaxed);
    }

    inline bool isEnabled()
    {
        return profiled_locks_enabled.load(std::memory_order_relaxed);
    }
}

inline void setProfiledLocksEnabled(bool enabled)
{
    ProfiledLocks::setEnabled(enabled);
}

inline bool areProfiledLocksEnabled()
{
    return ProfiledLocks::isEnabled();
}

/// RAII lock guard that measures both wait-time and hold-time for std::mutex.
/// Increments the wait-event after the lock is acquired, and the hold-event when the guard is destroyed.
class TSA_SCOPED_LOCKABLE ProfiledMutexLock final : private boost::noncopyable
{
public:
    ProfiledMutexLock(std::mutex & mutex, ProfileEvents::Event wait_event_, ProfileEvents::Event hold_event_) TSA_ACQUIRE(mutex)
        : wait_event(wait_event_)
        , hold_event(hold_event_)
        , profiled_locks_enabled(areProfiledLocksEnabled())
    {
        if (likely(!profiled_locks_enabled))
            lockImpl<false>(mutex);
        else
            lockImpl<true>(mutex);
    }

    ~ProfiledMutexLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
        {
            if (profiled_locks_enabled)
                unlockImpl<true>();
            else
                unlockImpl<false>();
        }
    }

    void unlock() TSA_RELEASE()
    {
        if (profiled_locks_enabled)
            unlockImpl<true>();
        else
            unlockImpl<false>();
    }

    void lock() TSA_ACQUIRE()
    {
        if (profiled_locks_enabled)
            relockImpl<true>();
        else
            relockImpl<false>();
    }

private:
    template <bool profiled_locks>
    void lockImpl(std::mutex & mutex)
    {
        if constexpr (profiled_locks)
        {
            Stopwatch wait_watch;
            std::unique_lock<std::mutex> l(mutex);
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            underlying_lock = std::move(l);
            if (hold_watch.has_value())
                hold_watch->restart();
            else
                hold_watch.emplace();
        }
        else
        {
            underlying_lock = std::unique_lock<std::mutex>(mutex);
        }
    }

    template <bool profiled_locks>
    void unlockImpl()
    {
        if constexpr (profiled_locks)
        {
            chassert(hold_watch.has_value());
            UInt64 elapsed = hold_watch->elapsedMicroseconds();
            underlying_lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
        else
        {
            underlying_lock.unlock();
        }
    }

    template <bool profiled_locks>
    void relockImpl()
    {
        if constexpr (profiled_locks)
        {
            Stopwatch wait_watch;
            underlying_lock.lock();
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            chassert(hold_watch.has_value());
            hold_watch->restart();
        }
        else
        {
            underlying_lock.lock();
        }
    }

    std::unique_lock<std::mutex> underlying_lock;
    ProfileEvents::Event wait_event;
    ProfileEvents::Event hold_event;
    bool profiled_locks_enabled;
    std::optional<Stopwatch> hold_watch;
};

/// RAII lock guard that measures both wait-time and hold-time for exclusive (write) locks on SharedMutex.
/// Increments the wait-event after the lock is acquired, and the hold-event when the guard is destroyed.
class TSA_SCOPED_LOCKABLE ProfiledExclusiveLock final : private boost::noncopyable
{
public:
    ProfiledExclusiveLock(SharedMutex & mutex, ProfileEvents::Event wait_event_, ProfileEvents::Event hold_event_) TSA_ACQUIRE(mutex)
        : wait_event(wait_event_)
        , hold_event(hold_event_)
        , profiled_locks_enabled(areProfiledLocksEnabled())
    {
        if (likely(!profiled_locks_enabled))
            lockImpl<false>(mutex);
        else
            lockImpl<true>(mutex);
    }

    ~ProfiledExclusiveLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
        {
            if (profiled_locks_enabled)
                unlockImpl<true>();
            else
                unlockImpl<false>();
        }
    }

    void unlock() TSA_RELEASE()
    {
        if (profiled_locks_enabled)
            unlockImpl<true>();
        else
            unlockImpl<false>();
    }

    void lock() TSA_ACQUIRE()
    {
        if (profiled_locks_enabled)
            relockImpl<true>();
        else
            relockImpl<false>();
    }

private:
    template <bool profiled_locks>
    void lockImpl(SharedMutex & mutex)
    {
        if constexpr (profiled_locks)
        {
            Stopwatch wait_watch;
            std::unique_lock<SharedMutex> l(mutex);
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            underlying_lock = std::move(l);
            if (hold_watch.has_value())
                hold_watch->restart();
            else
                hold_watch.emplace();
        }
        else
        {
            underlying_lock = std::unique_lock<SharedMutex>(mutex);
        }
    }

    template <bool profiled_locks>
    void unlockImpl()
    {
        if constexpr (profiled_locks)
        {
            chassert(hold_watch.has_value());
            UInt64 elapsed = hold_watch->elapsedMicroseconds();
            underlying_lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
        else
        {
            underlying_lock.unlock();
        }
    }

    template <bool profiled_locks>
    void relockImpl()
    {
        if constexpr (profiled_locks)
        {
            Stopwatch wait_watch;
            underlying_lock.lock();
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            chassert(hold_watch.has_value());
            hold_watch->restart();
        }
        else
        {
            underlying_lock.lock();
        }
    }

    std::unique_lock<SharedMutex> underlying_lock;
    ProfileEvents::Event wait_event;
    ProfileEvents::Event hold_event;
    bool profiled_locks_enabled;
    std::optional<Stopwatch> hold_watch;
};


/// RAII lock guard that measures both wait-time and hold-time for shared (read) locks on SharedMutex.
class TSA_SCOPED_LOCKABLE ProfiledSharedLock final : private boost::noncopyable
{
public:
    ProfiledSharedLock(SharedMutex & mutex, ProfileEvents::Event wait_event_, ProfileEvents::Event hold_event_) TSA_ACQUIRE_SHARED(mutex)
        : wait_event(wait_event_)
        , hold_event(hold_event_)
        , profiled_locks_enabled(areProfiledLocksEnabled())
    {
        if (likely(!profiled_locks_enabled))
            lockImpl<false>(mutex);
        else
            lockImpl<true>(mutex);
    }

    ~ProfiledSharedLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
        {
            if (profiled_locks_enabled)
                unlockImpl<true>();
            else
                unlockImpl<false>();
        }
    }

    void unlock() TSA_RELEASE()
    {
        if (profiled_locks_enabled)
            unlockImpl<true>();
        else
            unlockImpl<false>();
    }

    void lock() TSA_ACQUIRE()
    {
        if (profiled_locks_enabled)
            relockImpl<true>();
        else
            relockImpl<false>();
    }

private:
    template <bool profiled_locks>
    void lockImpl(SharedMutex & mutex)
    {
        if constexpr (profiled_locks)
        {
            Stopwatch wait_watch;
            std::shared_lock<SharedMutex> l(mutex);
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            underlying_lock = std::move(l);
            if (hold_watch.has_value())
                hold_watch->restart();
            else
                hold_watch.emplace();
        }
        else
        {
            underlying_lock = std::shared_lock<SharedMutex>(mutex);
        }
    }

    template <bool profiled_locks>
    void unlockImpl()
    {
        if constexpr (profiled_locks)
        {
            chassert(hold_watch.has_value());
            UInt64 elapsed = hold_watch->elapsedMicroseconds();
            underlying_lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
        else
        {
            underlying_lock.unlock();
        }
    }

    template <bool profiled_locks>
    void relockImpl()
    {
        if constexpr (profiled_locks)
        {
            Stopwatch wait_watch;
            underlying_lock.lock();
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            chassert(hold_watch.has_value());
            hold_watch->restart();
        }
        else
        {
            underlying_lock.lock();
        }
    }

    std::shared_lock<SharedMutex> underlying_lock;
    ProfileEvents::Event wait_event;
    ProfileEvents::Event hold_event;
    bool profiled_locks_enabled;
    std::optional<Stopwatch> hold_watch;
};

}
