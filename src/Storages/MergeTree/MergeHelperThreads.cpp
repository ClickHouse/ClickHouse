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
        if (used >= max_threads.load(std::memory_order_relaxed))
        {
            ProfileEvents::increment(ProfileEvents::MergeHelperThreadUnavailable);
            return nullptr;
        }
    } while (!used_threads.compare_exchange_weak(used, used + 1, std::memory_order_relaxed));

    return SlotPtr(new Slot);
}

void MergeHelperThreads::setMaxThreads(size_t max_threads_)
{
    max_threads.store(max_threads_, std::memory_order_relaxed);
}

}
