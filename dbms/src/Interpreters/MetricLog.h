#pragma once
#include <Interpreters/SystemLog.h>
#include <Interpreters/AsynchronousMetrics.h>

namespace DB
{

using Poco::Message;

struct MetricLogElement
{
    std::shared_ptr<AsynchronousMetrics> async_metrics_ptr{nullptr};

    static std::string name() { return "MetricLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class MetricLog : public SystemLog<MetricLogElement>
{
    using SystemLog<MetricLogElement>::SystemLog;
};

}
