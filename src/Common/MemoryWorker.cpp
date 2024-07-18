#include <Common/MemoryWorker.h>

#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
    extern const Event MemoryWorkerRun;
    extern const Event MemoryWorkerRunElapsedMicroseconds;
}

namespace DB
{

#if USE_JEMALLOC
#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

MemoryWorker::MemoryWorker(uint64_t period_ms_)
    : period_ms(period_ms_)
{
    background_thread = ThreadFromGlobalPool([this] { backgroundThread(); });
}

MemoryWorker::~MemoryWorker()
{
    {
        std::unique_lock lock(mutex);
        shutdown = true;
    }
    cv.notify_all();

    if (background_thread.joinable())
        background_thread.join();
}

void MemoryWorker::backgroundThread()
{
    JemallocMibCache<uint64_t> epoch_mib("epoch");
    JemallocMibCache<size_t> resident_mib("stats.resident");
    JemallocMibCache<size_t> allocated_mib("stats.allocated");
    JemallocMibCache<size_t> purge_mib("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge");
    std::unique_lock lock(mutex);
    while (true)
    {
        cv.wait_for(lock, period_ms, [this] { return shutdown; });
        if (shutdown)
            return;

        Stopwatch total_watch;
        epoch_mib.setValue(0);
        Int64 resident = resident_mib.getValue();
        if (resident > total_memory_tracker.getHardLimit())
        {
            Stopwatch purge_watch;
            purge_mib.run();
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, purge_watch.elapsedMicroseconds());
        }

        MemoryTracker::updateValues(resident, allocated_mib.getValue());
        ProfileEvents::increment(ProfileEvents::MemoryWorkerRun);
        ProfileEvents::increment(ProfileEvents::MemoryWorkerRunElapsedMicroseconds, total_watch.elapsedMicroseconds());
    }
}
#endif

}
