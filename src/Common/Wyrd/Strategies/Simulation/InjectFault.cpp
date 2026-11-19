#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/InjectFault.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Simulation.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>

namespace DB::Runtime::Wyrd
{

void injectFault(const char * operation, std::source_location location)
{
    sync(operation, location);
}

bool spuriousFailure(std::source_location location)
{
    if (auto * simulation = Simulation::current())
        return simulation->spuriousFailure(location);
    return false;
}

}
