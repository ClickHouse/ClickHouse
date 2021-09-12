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
}

