#pragma once

#include <Common/EventFD.h>

#include <atomic>
#include <list>
#include <memory>
#include <mutex>


namespace DB
{

/// The wake-up of a streaming query (`SELECT ... FROM memory_table STREAM`) when the data of a `Memory` table changes.
/// It can be waited on asynchronously, by polling `fd`.
class MemoryTableWakeup
{
public:
    /// Called on every change of the data, and on cancellation of the query.
    void notify();

    /// Called by the source before looking at the data.
    void reset();

#if defined(OS_LINUX) || defined(OS_DARWIN)
    [[nodiscard]] int fd() const { return event.fd; }
#endif

private:
    std::mutex mutex;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// Written once when the wake-up becomes signaled, read once when it is reset, so the reads never block.
    EventFD event;
#endif
    bool signaled = false;
};

using MemoryTableWakeupPtr = std::shared_ptr<MemoryTableWakeup>;

/// The streaming queries of a `Memory` table. A subscription ends when its source is destroyed.
class MemoryTableSubscribers
{
public:
    MemoryTableWakeupPtr subscribe();
    void notify();

private:
    std::mutex mutex;
    std::list<std::weak_ptr<MemoryTableWakeup>> wakeups;
    std::atomic<size_t> num_subscribers{0};
};

}
