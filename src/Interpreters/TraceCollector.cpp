#include "TraceCollector.h"

#include <Core/Field.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TraceLog.h>
#include <Poco/Logger.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>


namespace DB
{

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


TraceCollector::~TraceCollector()
{
    if (!thread.joinable())
        LOG_ERROR(&Poco::Logger::get("TraceCollector"), "TraceCollector thread is malformed and cannot be joined");
    else
        stop();

    TraceSender::pipe.close();
}


/** Sends TraceCollector stop message
  *
  * Each sequence of data for TraceCollector thread starts with a boolean flag.
  * If this flag is true, TraceCollector must stop reading trace_pipe and exit.
  * This function sends flag with a true value to stop TraceCollector gracefully.
  */
void TraceCollector::stop()
{
    WriteBufferFromFileDescriptor out(TraceSender::pipe.fds_rw[1]);
    writeChar(true, out);
    out.next();
    thread.join();
}


void TraceCollector::run()
{
    setThreadName("TraceCollector");

    ReadBufferFromFileDescriptor in(TraceSender::pipe.fds_rw[0]);

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
        in.read(query_id.data(), query_id_size);

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

        if (trace_log)
        {
            // time and time_in_microseconds are both being constructed from the same timespec so that the
            // times will be equal up to the precision of a second.
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);

            UInt64 time = static_cast<UInt64>(ts.tv_sec * 1000000000LL + ts.tv_nsec);
            UInt64 time_in_microseconds = static_cast<UInt64>((ts.tv_sec * 1000000LL) + (ts.tv_nsec / 1000));
            TraceLogElement element{time_t(time / 1000000000), time_in_microseconds, time, trace_type, thread_id, query_id, trace, size};
            trace_log->add(element);
        }
    }
}

}
