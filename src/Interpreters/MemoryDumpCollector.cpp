#include "MemoryDumpCollector.h"

#include <Core/Field.h>
#include <Interpreters/MemoryDumpLog.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/MemoryDumper.h>


namespace DB
{

MemoryDumpCollector::MemoryDumpCollector(std::shared_ptr<MemoryDumpLog> memory_dump_log_, UInt64 memory_dump_interval_ms_)
    : memory_dump_log(std::move(memory_dump_log_))
    , memory_dump_interval_ms(memory_dump_interval_ms_)
    , thread(ThreadFromGlobalPool(&MemoryDumpCollector::run, this))
{
    chassert(memory_dump_log);
}

MemoryDumpCollector::~MemoryDumpCollector()
{
    {
        std::lock_guard<std::mutex> lock(cond_mutex);
        stopped.store(true, std::memory_order_relaxed);
    }

    cond.notify_one();

    if (thread.joinable())
        thread.join();
    else
        LOG_ERROR(&Poco::Logger::get("MemoryDumpCollector"), "MemoryDumpCollector thread is malformed and cannot be joined");
}

void MemoryDumpCollector::run()
{
    setThreadName("MemoryDumper");

    std::vector<MemoryAllocation> result_memory_allocations;

    while (!stopped.load(std::memory_order_acquire))
    {
        {
            std::unique_lock<std::mutex> lock(cond_mutex);
            cond.wait_for(lock, std::chrono::milliseconds(memory_dump_interval_ms), [&]()
            {
                return stopped.load(std::memory_order_relaxed);
            });
        }

        if (stopped.load(std::memory_order_acquire))
            return;

        result_memory_allocations.clear();
        MemoryDumper::instance().dump(result_memory_allocations);

        LOG_TRACE(&Poco::Logger::get("MemoryDumpCollector"), "Dump {} allocations", result_memory_allocations.size());

        // time and time_in_microseconds are both being constructed from the same timespec so that the
        // times will be equal up to the precision of a second.
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);

        UInt64 time = static_cast<UInt64>(ts.tv_sec * 1000000000LL + ts.tv_nsec);
        UInt64 time_in_microseconds = static_cast<UInt64>((ts.tv_sec * 1000000LL) + (ts.tv_nsec / 1000));

        for (auto & memory_allocation : result_memory_allocations)
        {
            size_t stack_trace_size = memory_allocation.stack_trace.getSize();
            size_t stack_trace_offset = memory_allocation.stack_trace.getOffset();

            Array trace;
            for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
                trace.push_back(reinterpret_cast<uintptr_t>(memory_allocation.stack_trace.getFramePointers()[i]));

            MemoryDumpLogElement element{time_t(time / 1000000000),
                time_in_microseconds,
                time,
                memory_allocation.thread_id,
                memory_allocation.query_id,
                std::move(trace),
                memory_allocation.size,
                memory_allocation.ptr};

            memory_dump_log->add(std::move(element));
        }
    }
}

}
