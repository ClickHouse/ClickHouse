#include <Storages/MergeTree/MergeHelperThreads.h>

#include <Common/ProfileEvents.h>

namespace CurrentMetrics
{
    extern const Metric MergeHelperThreads;
}

namespace ProfileEvents
{
    extern const Event MergeHelperThreadUnavailable;
}

namespace DB
{

/// The default of the server setting `max_merge_helper_threads`, also used by `clickhouse-local`.
std::atomic<size_t> MergeHelperThreads::max_threads = 16;
std::atomic<size_t> MergeHelperThreads::used_threads = 0;

MergeHelperThreads::Slot::Slot()
    : metric_increment(CurrentMetrics::MergeHelperThreads)
{
}

MergeHelperThreads::Slot::~Slot()
{
    used_threads.fetch_sub(1, std::memory_order_relaxed);
}

MergeHelperThreads::SlotPtr MergeHelperThreads::tryAcquire()
{
    size_t used = used_threads.load(std::memory_order_relaxed);
    do
    {
        size_t max = max_threads.load(std::memory_order_relaxed);
        if (used >= max)
        {
            /// Zero disables the threads, so they are not unavailable.
            if (max)
                ProfileEvents::increment(ProfileEvents::MergeHelperThreadUnavailable);
            return nullptr;
        }
    } while (!used_threads.compare_exchange_weak(used, used + 1, std::memory_order_relaxed));

    /// The thread is already counted, and only the destructor of the slot releases it.
    try
    {
        return SlotPtr(new Slot);
    }
    catch (...)
    {
        used_threads.fetch_sub(1, std::memory_order_relaxed);
        throw;
    }
}

void MergeHelperThreads::setMaxThreads(size_t max_threads_)
{
    max_threads.store(max_threads_, std::memory_order_relaxed);
}

}
