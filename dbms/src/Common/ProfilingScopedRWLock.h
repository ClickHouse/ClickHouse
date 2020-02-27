#pragma once

#include <shared_mutex>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>


namespace DB
{

class ProfilingScoperWriteUnlocker;

class ProfilingScopedWriteRWLock
{
public:
    friend class ProfilingScoperWriteUnlocker;

    ProfilingScopedWriteRWLock(std::shared_mutex & rwl_, ProfileEvents::Event event_) :
        watch(),
        event(event_),
        scoped_write_lock(rwl_)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    ProfileEvents::Event event;
    std::unique_lock<std::shared_mutex> scoped_write_lock;
};

/// Inversed RAII
/// Used to unlock current writelock for various purposes.
class ProfilingScoperWriteUnlocker
{
public:
    ProfilingScoperWriteUnlocker() = delete;

    ProfilingScoperWriteUnlocker(ProfilingScopedWriteRWLock & parent_lock_) : parent_lock(parent_lock_)
    {
        parent_lock.scoped_write_lock.unlock();
    }

    ~ProfilingScoperWriteUnlocker()
    {
        Stopwatch watch;
        parent_lock.scoped_write_lock.lock();
        ProfileEvents::increment(parent_lock.event, watch.elapsed());
    }

private:
    ProfilingScopedWriteRWLock & parent_lock;
};

class ProfilingScopedReadRWLock
{
public:
    ProfilingScopedReadRWLock(std::shared_mutex & rwl, ProfileEvents::Event event) :
        watch(),
        scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::shared_lock<std::shared_mutex> scoped_read_lock;
};

}
