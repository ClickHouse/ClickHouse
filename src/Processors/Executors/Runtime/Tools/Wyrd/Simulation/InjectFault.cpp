#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/InjectFault.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Simulator.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Syscall.h>

namespace DB::Runtime::Wyrd
{

void injectFault(const char * operation, std::source_location location)
{
    sync(operation, location);
}

bool spuriousFailure(std::source_location location)
{
    if (auto * simulator = Simulator::current())
        return simulator->spuriousFailure(location);
    return false;
}

}
