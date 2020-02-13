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
#include <common/logger_useful.h>

#include <unistd.h>
#include <fcntl.h>


namespace ProfileEvents
{
    extern const Event QueryProfilerSignalOverruns;
}

namespace DB
{

namespace
{
    /// Normally query_id is a UUID (string with a fixed length) but user can provide custom query_id.
    /// Thus upper bound on query_id length should be introduced to avoid buffer overflow in signal handler.
    constexpr size_t QUERY_ID_MAX_LEN = 1024;

    thread_local size_t write_trace_iteration = 0;
}

namespace ErrorCodes
{
    extern const int NULL_POINTER_DEREFERENCE;
    extern const int THREAD_IS_NOT_JOINABLE;
}

TraceCollector::TraceCollector()
{
    pipe.open();

    /** Turn write end of pipe to non-blocking mode to avoid deadlocks
      * when QueryProfiler is invoked under locks and TraceCollector cannot pull data from pipe.
      */
    pipe.setNonBlocking();
    pipe.tryIncreaseSize(1 << 20);

    thread = ThreadFromGlobalPool(&TraceCollector::run, this);
}

TraceCollector::~TraceCollector()
{
    if (!thread.joinable())
        LOG_ERROR(&Poco::Logger::get("TraceCollector"), "TraceCollector thread is malformed and cannot be joined");
    else
    {
        stop();
        thread.join();
    }

    pipe.close();
}

void TraceCollector::collect(TraceType trace_type, const StackTrace & stack_trace, int overrun_count)
{
    /// Quickly drop if signal handler is called too frequently.
    /// Otherwise we may end up infinitelly processing signals instead of doing any useful work.
    ++write_trace_iteration;
    if (overrun_count)
    {
        /// But pass with some frequency to avoid drop of all traces.
        if (write_trace_iteration % overrun_count == 0)
        {
            ProfileEvents::increment(ProfileEvents::QueryProfilerSignalOverruns, overrun_count);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::QueryProfilerSignalOverruns, overrun_count + 1);
            return;
        }
    }

    constexpr size_t buf_size = sizeof(char) + // TraceCollector stop flag
        8 * sizeof(char) +                     // maximum VarUInt length for string size
        QUERY_ID_MAX_LEN * sizeof(char) +      // maximum query_id length
        sizeof(UInt8) +                        // number of stack frames
        sizeof(StackTrace::Frames) +           // collected stack trace, maximum capacity
        sizeof(TraceType) +                    // trace type
        sizeof(UInt64) +                       // thread_id
        sizeof(UInt64);                         // size
    char buffer[buf_size];
    WriteBufferFromFileDescriptorDiscardOnFailure out(pipe.fds_rw[1], buf_size, buffer);

    StringRef query_id = CurrentThread::getQueryId();
    query_id.size = std::min(query_id.size, QUERY_ID_MAX_LEN);

    auto thread_id = CurrentThread::get().thread_id;

    writeChar(false, out);
    writeStringBinary(query_id, out);

    size_t stack_trace_size = stack_trace.getSize();
    size_t stack_trace_offset = stack_trace.getOffset();
    writeIntBinary(UInt8(stack_trace_size - stack_trace_offset), out);
    for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
        writePODBinary(stack_trace.getFrames()[i], out);

    writePODBinary(trace_type, out);
    writePODBinary(thread_id, out);
    writePODBinary(UInt64(0), out);

    out.next();
}

void TraceCollector::collect(UInt64 size)
{
    constexpr size_t buf_size = sizeof(char) + // TraceCollector stop flag
        8 * sizeof(char) +                     // maximum VarUInt length for string size
        QUERY_ID_MAX_LEN * sizeof(char) +      // maximum query_id length
        sizeof(UInt8) +                        // number of stack frames
        sizeof(StackTrace::Frames) +           // collected stack trace, maximum capacity
        sizeof(TraceType) +                    // trace type
        sizeof(UInt64) +                       // thread_id
        sizeof(UInt64);                        // size
    char buffer[buf_size];
    WriteBufferFromFileDescriptorDiscardOnFailure out(pipe.fds_rw[1], buf_size, buffer);

    StringRef query_id = CurrentThread::getQueryId();
    query_id.size = std::min(query_id.size, QUERY_ID_MAX_LEN);

    auto thread_id = CurrentThread::get().thread_id;

    writeChar(false, out);
    writeStringBinary(query_id, out);

    const auto & stack_trace = StackTrace();

    size_t stack_trace_size = stack_trace.getSize();
    size_t stack_trace_offset = stack_trace.getOffset();
    writeIntBinary(UInt8(stack_trace_size - stack_trace_offset), out);
    for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
        writePODBinary(stack_trace.getFrames()[i], out);

    writePODBinary(TraceType::MEMORY, out);
    writePODBinary(thread_id, out);
    writePODBinary(size, out);

    out.next();
}

/**
  * Sends TraceCollector stop message
  *
  * Each sequence of data for TraceCollector thread starts with a boolean flag.
  * If this flag is true, TraceCollector must stop reading trace_pipe and exit.
  * This function sends flag with a true value to stop TraceCollector gracefully.
  *
  * NOTE: TraceCollector will NOT stop immediately as there may be some data left in the pipe
  *       before stop message.
  */
void TraceCollector::stop()
{
    WriteBufferFromFileDescriptor out(pipe.fds_rw[1]);
    writeChar(true, out);
    out.next();
}

void TraceCollector::run()
{
    ReadBufferFromFileDescriptor in(pipe.fds_rw[0]);

    while (true)
    {
        char is_last;
        readChar(is_last, in);
        if (is_last)
            break;

        std::string query_id;
        readStringBinary(query_id, in);

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

        UInt64 size;
        readPODBinary(size, in);

        if (trace_log)
        {
            TraceLogElement element{std::time(nullptr), trace_type, thread_id, query_id, trace, size};
            trace_log->add(element);
        }
    }
}

}
