#include "TraceCollector.h"

#include <Core/Field.h>
#include <Poco/Logger.h>
#include <common/Pipe.h>
#include <Common/StackTrace.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Common/Exception.h>
#include <Interpreters/TraceLog.h>

#include <unistd.h>
#include <fcntl.h>


namespace DB
{

LazyPipe trace_pipe;

namespace ErrorCodes
{
    extern const int NULL_POINTER_DEREFERENCE;
    extern const int THREAD_IS_NOT_JOINABLE;
    extern const int CANNOT_FCNTL;
}

TraceCollector::TraceCollector(std::shared_ptr<TraceLog> & trace_log_)
    : log(&Poco::Logger::get("TraceCollector"))
    , trace_log(trace_log_)
{
    if (trace_log == nullptr)
        throw Exception("Invalid trace log pointer passed", ErrorCodes::NULL_POINTER_DEREFERENCE);

    trace_pipe.open();

    /** Turn write end of pipe to non-blocking mode to avoid deadlocks
      * when QueryProfiler is invoked under locks and TraceCollector cannot pull data from pipe.
      */
    int flags = fcntl(trace_pipe.fds_rw[1], F_GETFL, 0);
    if (-1 == flags)
        throwFromErrno("Cannot get file status flags of pipe", ErrorCodes::CANNOT_FCNTL);
    if (-1 == fcntl(trace_pipe.fds_rw[1], F_SETFL, flags | O_NONBLOCK))
        throwFromErrno("Cannot set non-blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);

#if !defined(__FreeBSD__)
    /** Increase pipe size to avoid slowdown during fine-grained trace collection.
      */
    int pipe_size = fcntl(trace_pipe.fds_rw[1], F_GETPIPE_SZ);
    if (-1 == pipe_size)
    {
        if (errno == EINVAL)
        {
            LOG_INFO(log, "Cannot get pipe capacity, " << errnoToString(ErrorCodes::CANNOT_FCNTL) << ". Very old Linux kernels have no support for this fcntl.");
            /// It will work nevertheless.
        }
        else
            throwFromErrno("Cannot get pipe capacity", ErrorCodes::CANNOT_FCNTL);
    }
    else
    {
        constexpr int max_pipe_capacity_to_set = 1048576;
        for (errno = 0; errno != EPERM && pipe_size < max_pipe_capacity_to_set; pipe_size *= 2)
            if (-1 == fcntl(trace_pipe.fds_rw[1], F_SETPIPE_SZ, pipe_size * 2) && errno != EPERM)
                throwFromErrno("Cannot increase pipe capacity to " + toString(pipe_size * 2), ErrorCodes::CANNOT_FCNTL);

        LOG_TRACE(log, "Pipe capacity is " << formatReadableSizeWithBinarySuffix(std::min(pipe_size, max_pipe_capacity_to_set)));
    }
#endif

    thread = ThreadFromGlobalPool(&TraceCollector::run, this);
}

TraceCollector::~TraceCollector()
{
    if (!thread.joinable())
        LOG_ERROR(log, "TraceCollector thread is malformed and cannot be joined");
    else
    {
        TraceCollector::notifyToStop();
        thread.join();
    }

    trace_pipe.close();
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
void TraceCollector::notifyToStop()
{
    WriteBufferFromFileDescriptor out(trace_pipe.fds_rw[1]);
    writeChar(true, out);
    out.next();
}

void TraceCollector::run()
{
    ReadBufferFromFileDescriptor in(trace_pipe.fds_rw[0]);

    while (true)
    {
        char is_last;
        readChar(is_last, in);
        if (is_last)
            break;

        std::string query_id;
        readStringBinary(query_id, in);

        UInt8 size = 0;
        readIntBinary(size, in);

        Array trace;
        trace.reserve(size);

        for (size_t i = 0; i < size; i++)
        {
            uintptr_t addr = 0;
            readPODBinary(addr, in);
            trace.emplace_back(UInt64(addr));
        }

        TimerType timer_type;
        readPODBinary(timer_type, in);

        UInt32 thread_number;
        readPODBinary(thread_number, in);

        TraceLogElement element{std::time(nullptr), timer_type, thread_number, query_id, trace};
        trace_log->add(element);
    }
}

}
