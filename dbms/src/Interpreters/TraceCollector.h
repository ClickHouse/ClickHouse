#pragma once

#include <Poco/Runnable.h>
#include <Interpreters/Context.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

void NotifyTraceCollectorToStop();

class TraceCollector : public Poco::Runnable
{
private:
    Poco::Logger * log;
    std::shared_ptr<TraceLog> trace_log;

public:
    TraceCollector(std::shared_ptr<TraceLog> trace_log);

    void run() override;
};

}
