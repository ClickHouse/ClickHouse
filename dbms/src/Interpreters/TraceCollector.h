#pragma once

#include <Poco/Runnable.h>
#include <Poco/Logger.h>
#include <Common/Pipe.h>
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

    public:
        explicit TraceCollector(TraceLog * trace_log);

        void run() override;

        static const size_t buf_size;
    };

    class TracePipe : public ext::singleton<Pipe>
    {
    };
}
