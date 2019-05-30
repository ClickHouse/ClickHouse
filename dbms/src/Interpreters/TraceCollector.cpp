#include "TraceCollector.h"

#include <common/Sleep.h>
#include <common/Backtrace.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Common/Exception.h>
#include <Common/QueryProfiler.h>
#include <Interpreters/TraceLog.h>

namespace DB
{

    TraceCollector::TraceCollector(std::shared_ptr<TraceLog> trace_log)
        : log(&Logger::get("TraceCollector"))
        , trace_log(trace_log)
    {
    }

    void TraceCollector::run()
    {
        DB::ReadBufferFromFileDescriptor in(trace_pipe.fds_rw[0]);

        while (true)
        {
            SleepForMicroseconds(1);

            bool is_last;
            DB::readIntBinary(is_last, in);
            if (is_last)
                break;

            std::string query_id;
            Backtrace backtrace(NoCapture{});
            TimerType timer_type;

            DB::readStringBinary(query_id, in);
            DB::readPODBinary(backtrace, in);
            DB::readIntBinary(timer_type, in);

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
