#include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/Simulation.h>

namespace DB::Runtime::Wyrd
{

void sync(const char * operation, std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->sync(operation, location);
}

void yield(std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->yield(location);
}

void futexWait(void * address, std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->futexWait(address, location);
}

void futexWake(void * address, size_t count, std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->futexWake(address, count, location);
}

void spawn(std::function<void()> body, std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->spawn(std::move(body), location);
}

}
