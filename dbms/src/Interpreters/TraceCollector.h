#pragma once

#include <future>
#include <Poco/Runnable.h>
#include <Poco/Logger.h>
#include <ext/singleton.h>
#include <Interpreters/Context.h>

namespace DB
{
    using Poco::Logger;

    class TraceCollector : public Poco::Runnable
    {
    private:
        Logger * log;
        std::shared_ptr<TraceLog> trace_log;

    public:
        TraceCollector(std::shared_ptr<TraceLog> trace_log);

        void run() override;
    };
}
