#include "TraceCollector.h"

#include <Core/Field.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/TraceLog.h>
#include <Poco/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Common/HeapProfiler.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

TraceCollector::TraceCollector(std::shared_ptr<TraceLog> trace_log_)
    : trace_log(std::move(trace_log_))
{
    TraceSender::pipe.open();

    /** Turn write end of pipe to non-blocking mode to avoid deadlocks
      * when QueryProfiler is invoked under locks and TraceCollector cannot pull data from pipe.
      */
    TraceSender::pipe.setNonBlockingWrite();
    TraceSender::pipe.tryIncreaseSize(1 << 20);

    thread = ThreadFromGlobalPool(&TraceCollector::run, this);
}

void TraceCollector::tryClosePipe()
{
    try
    {
        TraceSender::pipe.close();
    }
    catch (...)
    {
        tryLogCurrentException("TraceCollector");
    }
}

TraceCollector::~TraceCollector()
{
    heap_profiler_dump_period_seconds.store(-1);

    try
    {
        /** Sends TraceCollector stop message
        *
        * Each sequence of data for TraceCollector thread starts with a boolean flag.
        * If this flag is true, TraceCollector must stop reading trace_pipe and exit.
        * This function sends flag with a true value to stop TraceCollector gracefully.
        */
        WriteBufferFromFileDescriptor out(TraceSender::pipe.fds_rw[1]);
        writeChar(true, out);
        out.next();
    }
    catch (...)
    {
        tryLogCurrentException("TraceCollector");
    }

    tryClosePipe();

    if (heap_profiler_task)
        heap_profiler_task->deactivate();

    if (thread.joinable())
        thread.join();
    else
        LOG_ERROR(&Poco::Logger::get("TraceCollector"), "TraceCollector thread is malformed and cannot be joined");
}


static UInt64 currentTimeNs()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<UInt64>(ts.tv_sec * 1000000000LL + ts.tv_nsec);
}

void TraceCollector::run()
{
    setThreadName("TraceCollector");

    ReadBufferFromFileDescriptor in(TraceSender::pipe.fds_rw[0]);

    try
    {
        while (true)
        {
            char is_last;
            readChar(is_last, in);
            if (is_last)
                break;

            std::string query_id;
            UInt8 query_id_size = 0;
            readBinary(query_id_size, in);
            query_id.resize(query_id_size);
            in.readStrict(query_id.data(), query_id_size);

            UInt8 trace_size = 0;
            readIntBinary(trace_size, in);

            Array trace;
            trace.reserve(trace_size);

            for (size_t i = 0; i < trace_size; ++i)
            {
                uintptr_t addr = 0;
                readPODBinary(addr, in);
                trace.emplace_back(static_cast<UInt64>(addr));
            }

            TraceType trace_type;
            readPODBinary(trace_type, in);

            UInt64 thread_id;
            readPODBinary(thread_id, in);

            Int64 size;
            readPODBinary(size, in);

            UInt64 ptr;
            readPODBinary(ptr, in);

            ProfileEvents::Event event;
            readPODBinary(event, in);

            ProfileEvents::Count increment;
            readPODBinary(increment, in);

            if (trace_log)
            {
                // time and time_in_microseconds are both being constructed from the same timespec so that the
                // times will be equal up to the precision of a second.
                UInt64 time = currentTimeNs();
                TraceLogElement element{time_t(time / 1000000000), time / 1000, time, trace_type, thread_id, query_id, trace, size, ptr, event, increment, 1, std::nullopt};
                trace_log->add(std::move(element));
            }
        }
    }
    catch (...)
    {
        tryClosePipe();
        throw;
    }
}

void TraceCollector::setHeapProfilerDumpPeriod(Int64 seconds)
{
    if (!HeapProfiler::instance().enabled())
        return;

    if (heap_profiler_dump_period_seconds.exchange(seconds) == seconds)
        return;

    if (seconds < 0)
        return;

    if (!heap_profiler_task)
        heap_profiler_task = trace_log->getContext()->getSchedulePool().createTask(
            "heap-profiler", [this] { heapProfilerTask(); });

    scheduleHeapProfilerTask();
}

void TraceCollector::dumpHeapProfileNow()
{
    if (!HeapProfiler::instance().enabled())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Heap profiler is disabled (heap_profiler_log_sample_rate)");

    if (!heap_profiler_task)
        heap_profiler_task = trace_log->getContext()->getSchedulePool().createTask(
            "heap-profiler", [this] { heapProfilerTask(); });

    manual_heap_dump_requested.store(true);

    scheduleHeapProfilerTask();

    // Wait for the dumping to complete. If multiple such requests happen concurrently, it's not
    // guaranteed that we'll wait for the dump that *we* requested; it could be another manual dump
    // that started earlier.
    std::unique_lock lock(manual_heap_dump_mutex);
    manual_heap_dump_cv.wait(lock, [&] { return !manual_heap_dump_requested; });
}

void TraceCollector::scheduleHeapProfilerTask()
{
    if (!heap_profiler_task)
        return;
    if (manual_heap_dump_requested.load())
    {
        heap_profiler_task->schedule();
        return;
    }
    Int64 seconds = heap_profiler_dump_period_seconds.load();
    if (seconds >= 0)
        heap_profiler_task->scheduleAfter(size_t(seconds * 1000));
}

void TraceCollector::heapProfilerTask()
{
    bool requested_manually = manual_heap_dump_requested.load();
    if (!requested_manually && heap_profiler_dump_period_seconds.load() < 0)
        return;

    auto complete = [&] {
        if (requested_manually)
        {
            std::unique_lock lock(manual_heap_dump_mutex);
            manual_heap_dump_requested.store(false);
            manual_heap_dump_cv.notify_all();
        }
        scheduleHeapProfilerTask();
    };

    try
    {
        auto samples = HeapProfiler::instance().dump();
        UInt64 time = currentTimeNs();
        UUID profile_id = UUIDHelpers::generateV4();

        std::vector<TraceLogElement> elements;
        for (const auto & sample : samples)
        {
            if (sample.weight == 0)
                continue;

            Array trace;
            trace.reserve(sample.stack.getSize() - sample.stack.getOffset());
            for (size_t i = sample.stack.getOffset(); i < sample.stack.getSize(); ++i)
                trace.emplace_back(reinterpret_cast<UInt64>(sample.stack.getFramePointers()[i]));

            TraceLogElement element{
                .event_time = time_t(time / 1000000000),
                .event_time_microseconds = time / 1000,
                .timestamp_ns = time,
                .trace_type = TraceType::MemoryProfile,
                .thread_id = sample.thread_id,
                .query_id = std::string(sample.query_id.data(), sample.query_id_len),
                .trace = trace,
                .size = Int64(sample.size),
                .ptr = UInt64(sample.ptr),
                .event = ProfileEvents::end(),
                .increment = 0,
                .weight = sample.weight,
                .profile_id = profile_id};
            elements.emplace_back(std::move(element));
        }

        trace_log->addGroup(std::move(elements));
    }
    catch (...)
    {
        complete();
        throw;
    }

    complete();
}

}
