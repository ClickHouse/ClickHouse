#pragma once

#include <Common/Threading.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>


namespace DB
{

class ProfilingScopedWriteRWLock
{
public:

    ProfilingScopedWriteRWLock(DB::FastSharedMutex & rwl_, ProfileEvents::Event event) :
        scoped_write_lock(rwl_)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::unique_lock<DB::FastSharedMutex> scoped_write_lock;
};


class ProfilingScopedReadRWLock
{
public:
    ProfilingScopedReadRWLock(DB::FastSharedMutex & rwl, ProfileEvents::Event event) :
        scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::shared_lock<DB::FastSharedMutex> scoped_read_lock;
};

}
