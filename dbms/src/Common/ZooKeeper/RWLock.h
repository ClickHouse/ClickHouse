#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/Exception.h>
#include <Poco/Event.h>
#include <string>
#include <type_traits>
#include <functional>

namespace zkutil
{

/** Distributed read/write lock for ZooKeeper.
  * Such a RWLock object may not be shared among threads.
  */
class RWLock final
{
public:
    enum Type
    {
        Read = 0,
        Write
    };

    enum Mode
    {
        Blocking = 0,
        NonBlocking
    };

    using CancellationHook = std::function<void()>;

public:
    RWLock() = default;

    /// Create under the specified ZooKeeper path a queue for lock requests
    /// if it doesn't exist yet.
    RWLock(GetZooKeeper get_zookeeper_, const std::string & path_);

    RWLock(const RWLock &) = delete;
    RWLock & operator=(const RWLock &) = delete;

    RWLock(RWLock &&) = default;
    RWLock & operator=(RWLock &&) = default;

    operator bool() const;

    /// Register a function that cancels lock operations if requested.
    void setCancellationHook(CancellationHook cancellation_hook_);

    /// Get a read lock.
    void acquireRead(RWLock::Mode mode = RWLock::Blocking);

    /// Get a write lock.
    void acquireWrite(RWLock::Mode mode = RWLock::Blocking);

    /// Check whether we have acquired the lock.
    bool ownsLock() const;

    /// Release lock.
    void release();

public:
    template <Type, Mode = Blocking> class Guard;

private:
    /// Cancel any ongoing operation if requested.
    template <RWLock::Type lock_type> void acquireImpl(RWLock::Mode mode);
    void abortIfRequested();

private:
    /// Helper that acquires an alive ZooKeeper session.
    GetZooKeeper get_zookeeper;
    EventPtr event = std::make_shared<Poco::Event>();
    /// Function that cancels barrier operations if requested.
    CancellationHook cancellation_hook;
    /// Path to the lock request queue.
    std::string path;
    /// Identifier of our request for a lock.
    std::string key;
    bool owns_lock = false;
};

/** Scoped version of RWLock.
  */
template <RWLock::Type lock_type, RWLock::Mode lock_mode>
class RWLock::Guard final
{
    static_assert((lock_type == RWLock::Read) || (lock_type == RWLock::Write), "Invalid RWLock type");
    static_assert((lock_mode == RWLock::Blocking) || (lock_mode == RWLock::NonBlocking), "Invalid RWLock mode");

public:
    /// Acquire lock.
    Guard(RWLock & rw_lock_)
        : rw_lock(rw_lock_)
    {
        if (lock_type == RWLock::Read)
            rw_lock.acquireRead(lock_mode);
        else if (lock_type == RWLock::Write)
            rw_lock.acquireWrite(lock_mode);
    }

    /// Release lock.
    ~Guard()
    {
        if (rw_lock.ownsLock())
        {
            try
            {
                rw_lock.release();
            }
            catch (...)
            {
                DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    Guard(const Guard &) = delete;
    Guard & operator=(const Guard &) = delete;

private:
    RWLock & rw_lock;
};

}
