#include "TraceCollector.h"

#include <Core/Field.h>
#include <Poco/Logger.h>
#include <common/StackTrace.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Common/Exception.h>
#include <Common/QueryProfiler.h>
#include <Interpreters/TraceLog.h>

using namespace DB;

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
void DB::NotifyTraceCollectorToStop()
{
    WriteBufferFromFileDescriptor out(trace_pipe.fds_rw[1]);
    writeIntBinary(true, out);
    out.next();
}

TraceCollector::TraceCollector(std::shared_ptr<TraceLog> trace_log)
    : log(&Poco::Logger::get("TraceCollector"))
    , trace_log(trace_log)
{
    if (trace_log == nullptr)
        throw Poco::Exception("Invalid trace log pointer passed");
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
        StackTrace stack_trace(NoCapture{});
        TimerType timer_type;

        readStringBinary(query_id, in);
        readPODBinary(stack_trace, in);
        readPODBinary(timer_type, in);

        const auto size = stack_trace.getSize();
        const auto& frames = stack_trace.getFrames();

        Array trace;
        trace.reserve(size);
        for (size_t i = 0; i < size; i++)
            trace.emplace_back(UInt64(reinterpret_cast<uintptr_t>(frames[i])));

        TraceLogElement element{std::time(nullptr), timer_type, query_id, trace};

        trace_log->add(element);
    }
}
