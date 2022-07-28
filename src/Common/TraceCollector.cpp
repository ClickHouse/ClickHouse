#include "TraceCollector.h"

#include <Core/Field.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TraceLog.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/PipeFDs.h>
#include <Common/StackTrace.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>


namespace DB
{

namespace
{
    /// Normally query_id is a UUID (string with a fixed length) but user can provide custom query_id.
    /// Thus upper bound on query_id length should be introduced to avoid buffer overflow in signal handler.
    ///
    /// And it cannot be large, since otherwise it will not fit into PIPE_BUF.
    /// The performance test query ids can be surprisingly long like
    /// `aggregating_merge_tree_simple_aggregate_function_string.query100.profile100`,
    /// so make some allowance for them as well.
    constexpr size_t QUERY_ID_MAX_LEN = 128;
    static_assert(QUERY_ID_MAX_LEN <= std::numeric_limits<uint8_t>::max());
}

LazyPipeFDs pipe;


TraceCollector::TraceCollector(std::shared_ptr<TraceLog> trace_log_)
    : trace_log(std::move(trace_log_))
{
    pipe.open();

    /** Turn write end of pipe to non-blocking mode to avoid deadlocks
      * when QueryProfiler is invoked under locks and TraceCollector cannot pull data from pipe.
      */
    pipe.setNonBlockingWrite();
    pipe.tryIncreaseSize(1 << 20);

    thread = ThreadFromGlobalPool(&TraceCollector::run, this);
}


TraceCollector::~TraceCollector()
{
    if (!thread.joinable())
        LOG_ERROR(&Poco::Logger::get("TraceCollector"), "TraceCollector thread is malformed and cannot be joined");
    else
        stop();

    pipe.close();
}


void TraceCollector::collect(TraceType trace_type, const StackTrace & stack_trace, Int64 size)
{
    constexpr size_t buf_size = sizeof(char) /// TraceCollector stop flag
        + sizeof(UInt8)                      /// String size
        + QUERY_ID_MAX_LEN                   /// Maximum query_id length
        + sizeof(UInt8)                      /// Number of stack frames
        + sizeof(StackTrace::FramePointers)  /// Collected stack trace, maximum capacity
        + sizeof(TraceType)                  /// trace type
        + sizeof(UInt64)                     /// thread_id
        + sizeof(Int64);                     /// size

    /// Write should be atomic to avoid overlaps
    /// (since recursive collect() is possible)
    static_assert(PIPE_BUF >= 512);
    static_assert(buf_size <= 512, "Only write of PIPE_BUF to pipe is atomic and the minimal known PIPE_BUF across supported platforms is 512");

    char buffer[buf_size];
    WriteBufferFromFileDescriptorDiscardOnFailure out(pipe.fds_rw[1], buf_size, buffer);

    StringRef query_id;
    UInt64 thread_id;

    if (CurrentThread::isInitialized())
    {
        query_id = CurrentThread::getQueryId();
        query_id.size = std::min(query_id.size, QUERY_ID_MAX_LEN);

        thread_id = CurrentThread::get().thread_id;
    }
    else
    {
        thread_id = MainThreadStatus::get()->thread_id;
    }

    writeChar(false, out);  /// true if requested to stop the collecting thread.

    writeBinary(static_cast<uint8_t>(query_id.size), out);
    out.write(query_id.data, query_id.size);

    size_t stack_trace_size = stack_trace.getSize();
    size_t stack_trace_offset = stack_trace.getOffset();
    writeIntBinary(UInt8(stack_trace_size - stack_trace_offset), out);
    for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
        writePODBinary(stack_trace.getFramePointers()[i], out);

    writePODBinary(trace_type, out);
    writePODBinary(thread_id, out);
    writePODBinary(size, out);

    out.next();
}


/** Sends TraceCollector stop message
  *
  * Each sequence of data for TraceCollector thread starts with a boolean flag.
  * If this flag is true, TraceCollector must stop reading trace_pipe and exit.
  * This function sends flag with a true value to stop TraceCollector gracefully.
  */
void TraceCollector::stop()
{
    WriteBufferFromFileDescriptor out(pipe.fds_rw[1]);
    writeChar(true, out);
    out.next();
    thread.join();
}


void TraceCollector::run()
{
    setThreadName("TraceCollector");

    ReadBufferFromFileDescriptor in(pipe.fds_rw[0]);

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

        for (size_t i = 0; i < trace_size; i++)
        {
            uintptr_t addr = 0;
            readPODBinary(addr, in);
            trace.emplace_back(UInt64(addr));
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

            UInt64 time = UInt64(ts.tv_sec * 1000000000LL + ts.tv_nsec);
            UInt64 time_in_microseconds = UInt64((ts.tv_sec * 1000000LL) + (ts.tv_nsec / 1000));
            TraceLogElement element{time_t(time / 1000000000), time_in_microseconds, time, trace_type, thread_id, query_id, trace, size};
            trace_log->add(element);
        }
    }
}

}
