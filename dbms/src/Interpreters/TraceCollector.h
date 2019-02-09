#pragma once

#include <future>
#include <Poco/Runnable.h>
#include <Poco/Logger.h>
#include <common/Pipe.h>
#include <ext/singleton.h>
#include <Interpreters/Context.h>
#include <Interpreters/TraceLog.h>

namespace DB
{
    using Poco::Logger;

    class TraceCollector : public Poco::Runnable
    {
    private:
        Logger * log;
        TraceLog * trace_log;
        std::future<void> stop_future;

    public:
        explicit TraceCollector(TraceLog * trace_log, std::future<void>&& stop_future);

        void run() override;
    };

    extern LazyPipe trace_pipe;
}
