#include <Common/CgroupsMemoryUsageObserver.h>

#if defined(OS_LINUX)

#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <base/cgroupsv2.h>
#include <base/getMemoryAmount.h>
#include <base/sleep.h>
#include <fmt/ranges.h>

#include <cstdint>

using namespace DB;

namespace DB
{

CgroupsMemoryUsageObserver::CgroupsMemoryUsageObserver(std::chrono::seconds wait_time_)
    : log(getLogger("CgroupsMemoryUsageObserver")), wait_time(wait_time_)
{}

CgroupsMemoryUsageObserver::~CgroupsMemoryUsageObserver()
{
    stopThread();
}

void CgroupsMemoryUsageObserver::setOnMemoryAmountAvailableChangedFn(OnMemoryAmountAvailableChangedFn on_memory_amount_available_changed_)
{
    std::lock_guard<std::mutex> memory_amount_available_changed_lock(memory_amount_available_changed_mutex);
    on_memory_amount_available_changed = on_memory_amount_available_changed_;
}

void CgroupsMemoryUsageObserver::startThread()
{
    if (!thread.joinable())
    {
        thread = ThreadFromGlobalPool(&CgroupsMemoryUsageObserver::runThread, this);
        LOG_INFO(log, "Started cgroup current memory usage observer thread");
    }
}

void CgroupsMemoryUsageObserver::stopThread()
{
    {
        std::lock_guard lock(thread_mutex);
        if (!thread.joinable())
            return;
        quit = true;
    }

    cond.notify_one();
    thread.join();

    LOG_INFO(log, "Stopped cgroup current memory usage observer thread");
}

void CgroupsMemoryUsageObserver::runThread()
{
    setThreadName("CgrpMemUsgObsr");

    last_available_memory_amount = getMemoryAmount();
    LOG_INFO(log, "Memory amount initially available to the process is {}", ReadableSize(last_available_memory_amount));

    std::unique_lock lock(thread_mutex);
    while (true)
    {
        if (cond.wait_for(lock, wait_time, [this] { return quit; }))
            break;

        try
        {
            uint64_t available_memory_amount = getMemoryAmount();
            if (available_memory_amount != last_available_memory_amount)
            {
                LOG_INFO(log, "Memory amount available to the process changed from {} to {}", ReadableSize(last_available_memory_amount), ReadableSize(available_memory_amount));
                last_available_memory_amount = available_memory_amount;
                std::lock_guard<std::mutex> memory_amount_available_changed_lock(memory_amount_available_changed_mutex);
                on_memory_amount_available_changed();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

}

#endif
