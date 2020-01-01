#include "TraceCollector.h"

#include <Core/Field.h>
#include <Poco/Logger.h>
#include <Common/PipeFDs.h>
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

LazyPipeFDs trace_pipe;

namespace ErrorCodes
{
    extern const int NULL_POINTER_DEREFERENCE;
    extern const int THREAD_IS_NOT_JOINABLE;
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
    trace_pipe.setNonBlocking();
    trace_pipe.tryIncreaseSize(1 << 20);

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
