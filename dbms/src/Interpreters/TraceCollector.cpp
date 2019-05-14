#include "TraceCollector.h"

#include <common/Backtrace.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Common/Exception.h>
#include <Common/QueryProfiler.h>
#include <Interpreters/TraceLog.h>


namespace DB
{
    LazyPipe trace_pipe;

    TraceCollector::TraceCollector(TraceLog * trace_log, std::future<void>&& stop_future)
        : log(&Logger::get("TraceCollector"))
        , trace_log(trace_log)
        , stop_future(std::move(stop_future))
    {
    }

    void TraceCollector::run()
    {
        DB::ReadBufferFromFileDescriptor in(trace_pipe.fds_rw[0]);

        while (stop_future.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout)
        {
            Backtrace backtrace;
            std::string query_id;
            TimerType timer_type;

            try {
                DB::readPODBinary(backtrace, in);
                DB::readStringBinary(query_id, in);
                DB::readIntBinary(timer_type, in);
            }
            catch (...)
            {
                /// Pipe was closed - looks like server is about to shutdown
                /// Let us wait for stop_future
                continue;
            }

            if (trace_log != nullptr)
            {
                const auto size = backtrace.getSize();
                const auto& frames = backtrace.getFrames();

                std::vector<UInt64> trace;
                trace.reserve(size);
                for (size_t i = 0; i < size; i++)
                    trace.push_back(reinterpret_cast<uintptr_t>(frames[i]));

                TraceLogElement element{std::time(nullptr), timer_type, query_id, trace};

                trace_log->add(element);
            }
        }
    }
}
