#pragma once

#include <Common/SharedMutex.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <shared_mutex>


namespace DB
{

class ProfilingScopedWriteRWLock
{
public:

    ProfilingScopedWriteRWLock(SharedMutex & rwl_, ProfileEvents::Event event) :
        scoped_write_lock(rwl_)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::unique_lock<SharedMutex> scoped_write_lock;
};


class ProfilingScopedReadRWLock
{
public:
    ProfilingScopedReadRWLock(SharedMutex & rwl, ProfileEvents::Event event) :
        scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::shared_lock<SharedMutex> scoped_read_lock;
};

}
