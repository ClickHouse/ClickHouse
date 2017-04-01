#pragma once

#include <Poco/RWLock.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

/*
 TODO: replace locks with std::shared_mutex - std::unique_lock - std::shared_lock when c++17
*/

namespace DB
{
class ProfilingScopedWriteRWLock
{
public:
    ProfilingScopedWriteRWLock(Poco::RWLock & rwl, ProfileEvents::Event event) :
        watch(),
        scoped_write_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:

    Stopwatch watch;
    Poco::ScopedWriteRWLock scoped_write_lock;
};

class ProfilingScopedReadRWLock
{
public:
    ProfilingScopedReadRWLock(Poco::RWLock & rwl, ProfileEvents::Event event) :
        watch(),
        scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:

    Stopwatch watch;
    Poco::ScopedReadRWLock scoped_read_lock;
};

}
