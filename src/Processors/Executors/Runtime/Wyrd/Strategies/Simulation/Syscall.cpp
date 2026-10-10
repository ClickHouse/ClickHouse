#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Simulation.h>

#include <stdexcept>

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

void enterLockFreeScope(std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->enterLockFreeScope(location);
}

void leaveLockFreeScope(std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->leaveLockFreeScope(location);
}

void progressHint(std::source_location location)
{
    if (auto * simulation = Simulation::current())
        simulation->progressHint(location);
}

void trace(std::string message)
{
    if (auto * simulation = Simulation::current())
        simulation->trace(std::move(message));
}

void brokenInvariant(std::string message)
{
    if (auto * simulation = Simulation::current())
        simulation->brokenInvariant(std::move(message));
}

}
