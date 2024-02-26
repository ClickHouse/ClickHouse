#include <iostream>
#include <Processors/IProcessor.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Common/CurrentThread.h>

namespace DB
{

void IProcessor::dump() const
{
    std::cerr << getName() << "\n";

    std::cerr << "inputs:\n";
    for (const auto & port : inputs)
        std::cerr << "\t" << port.hasData() << " " << port.isFinished() << "\n";

    std::cerr << "outputs:\n";
    for (const auto & port : outputs)
        std::cerr << "\t" << port.hasData() << " " << port.isNeeded() << "\n";
}


std::string IProcessor::statusToName(Status status)
{
    switch (status)
    {
        case Status::NeedData:
            return "NeedData";
        case Status::PortFull:
            return "PortFull";
        case Status::Finished:
            return "Finished";
        case Status::Ready:
            return "Ready";
        case Status::Async:
            return "Async";
        case Status::ExpandPipeline:
            return "ExpandPipeline";
    }

    UNREACHABLE();
}

void IProcessor::process(bool trace_processors)
{
    if (this->workHook)
        this->workHook->onEnter();

    OpenTelemetry::SpanHolderPtr span;
    if (trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(demangle(typeid(*this).name()) + "::work()");
        span->addAttribute("clickhouse.thread_id", CurrentThread::get().thread_id);
    }

    try
    {
        work();

        /// Make sure the span is closed before the hook
        span.reset();
    }
    catch (...)
    {
        if (this->workHook)
            this->workHook->onLeave();

        throw;
    }

    if (this->workHook)
        this->workHook->onLeave();
}


}

