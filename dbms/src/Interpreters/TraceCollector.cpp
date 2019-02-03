#include "TraceCollector.h"
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>

namespace DB {

    const size_t TraceCollector::buf_size = sizeof(int);

    TraceCollector::TraceCollector()
        : log(&Logger::get("TraceCollector"))
    {
    }

    void TraceCollector::run()
    {
        LOG_INFO(log, "TraceCollector started");

        char buf[buf_size];
        DB::ReadBufferFromFileDescriptor in(PipeSingleton::instance().read_fd, buf_size, buf);

        LOG_INFO(log, "Preparing to read from: " << PipeSingleton::instance().read_fd);

        while (true)
        {
            int sig = 0;
            DB::readBinary(sig, in);

            LOG_INFO(log, "Received signal: " << sig);
        }

        LOG_INFO(log, "TraceCollector exited");
    }

}
