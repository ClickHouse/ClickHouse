#pragma once

#include <shared_mutex>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>


namespace DB
{

class ProfilingScopedWriteRWLock
{
public:

    ProfilingScopedWriteRWLock(std::shared_mutex & rwl_, ProfileEvents::Event event) :
        scoped_write_lock(rwl_)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::unique_lock<std::shared_mutex> scoped_write_lock;
};


class ProfilingScopedReadRWLock
{
public:
    ProfilingScopedReadRWLock(std::shared_mutex & rwl, ProfileEvents::Event event) :
        scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::shared_lock<std::shared_mutex> scoped_read_lock;
};

}
