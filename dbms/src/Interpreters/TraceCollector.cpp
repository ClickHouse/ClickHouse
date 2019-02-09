#include "TraceCollector.h"

#include <common/Backtrace.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Common/Exception.h>


namespace DB {
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
            ucontext_t context;
            std::string query_id;

            try {
                DB::readPODBinary(context, in);
                DB::readStringBinary(query_id, in);
            } catch (Exception) {
                /// Pipe was closed - looks like server is about to shutdown
                /// Let us wait for stop_future
                continue;
            }

            if (trace_log != nullptr) {
                std::vector<void *> frames = getBacktraceFrames(context);
                std::vector<UInt64> trace;
                trace.reserve(frames.size());
                for (void * frame : frames) {
                    trace.push_back(reinterpret_cast<uintptr_t>(frame));
                }

                TraceLogElement element{std::time(nullptr), query_id, trace};

                trace_log->add(element);
            }
        }
    }
}
