#include "TraceCollector.h"
#include <common/Backtrace.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <ctime>


namespace DB {

    const size_t TraceCollector::buf_size = sizeof(int) + sizeof(siginfo_t) + sizeof(ucontext_t);

    TraceCollector::TraceCollector(TraceLog * trace_log)
        : log(&Logger::get("TraceCollector"))
        , trace_log(trace_log)
    {
    }

    void TraceCollector::run()
    {
        DB::ReadBufferFromFileDescriptor in(TracePipe::instance().read_fd);

        while (true)
        {
            ucontext_t context;
            std::string queryID;

            DB::readPODBinary(context, in);
            DB::readStringBinary(queryID, in);

            LOG_INFO(log, queryID);

            if (trace_log != nullptr) {
                std::vector<void *> frames = getBacktraceFrames(context);
                std::vector<UInt64> trace;
                trace.reserve(frames.size());
                for (void * frame : frames) {
                    trace.push_back(reinterpret_cast<uintptr_t>(frame));
                }

                TraceLogElement element{std::time(nullptr), queryID, trace};

                trace_log->add(element);

                LOG_INFO(log, "TraceCollector added row");
            }
        }

        LOG_INFO(log, "TraceCollector exited");
    }

}
