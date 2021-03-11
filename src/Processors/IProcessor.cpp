#include <iostream>
#include <Processors/IProcessor.h>


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
        case Status::Wait:
            return "Wait";
        case Status::ExpandPipeline:
            return "ExpandPipeline";
    }

    __builtin_unreachable();
}

}

