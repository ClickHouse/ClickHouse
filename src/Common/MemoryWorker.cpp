#include <Common/MemoryWorker.h>

#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <Common/formatReadable.h>

namespace DB
{

#if USE_JEMALLOC
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
    std::unique_lock lock(mutex);
    while (true)
    {
        cv.wait_for(lock, period_ms, [this] { return shutdown; });
        if (shutdown)
            return;

        epoch_mib.setValue(0);
        Int64 resident = resident_mib.getValue();
        MemoryTracker::setRSS(resident);
        if (resident > total_memory_tracker.getHardLimit())
            purgeJemallocArenas();
    }
}
#endif

}
