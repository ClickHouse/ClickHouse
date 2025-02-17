#include <iostream>
#include <Processors/IProcessor.h>

#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void IProcessor::cancel() noexcept
{

    bool already_cancelled = is_cancelled.exchange(true, std::memory_order_acq_rel);
    if (already_cancelled)
        return;

    onCancel();
}

String IProcessor::debug() const
{
    WriteBufferFromOwnString buf;
    writeString(getName(), buf);
    buf.write('\n');

    writeString("inputs (hasData, isFinished):\n", buf);
    for (const auto & port : inputs)
    {
        buf.write('\t');
        writeBoolText(port.hasData(), buf);
        buf.write(' ');
        writeBoolText(port.isFinished(), buf);
        buf.write('\n');
    }

    writeString("outputs (hasData, isNeeded):\n", buf);
    for (const auto & port : outputs)
    {
        buf.write('\t');
        writeBoolText(port.hasData(), buf);
        buf.write(' ');
        writeBoolText(port.isNeeded(), buf);
        buf.write('\n');
    }

    buf.finalize();
    return buf.str();
}

void IProcessor::dump() const
{
    std::cerr << debug();
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
}

}
