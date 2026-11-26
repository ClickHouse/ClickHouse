#include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#include <Common/Wyrd/Strategies/Simulation/Simulation.h>

#include <stdexcept>

namespace DB::Wyrd::SimulationMode
{

void sync(const char * operation, std::source_location location)
{
    Simulation::current()->sync(operation, location);
}

void yield(std::source_location location)
{
    Simulation::current()->yield(location);
}

void futexWait(void * address, std::source_location location)
{
    Simulation::current()->futexWait(address, location);
}

void futexWake(void * address, size_t count, std::source_location location)
{
    Simulation::current()->futexWake(address, count, location);
}

void spawn(std::function<void()> body, std::source_location location)
{
    Simulation::current()->spawn(std::move(body), location);
}

void enterLockFreeScope(std::source_location location)
{
    Simulation::current()->enterLockFreeScope(location);
}

void leaveLockFreeScope(std::source_location location)
{
    Simulation::current()->leaveLockFreeScope(location);
}

void progressHint(std::source_location location)
{
    Simulation::current()->progressHint(location);
}

void trace(std::string message)
{
    Simulation::current()->trace(std::move(message));
}

void brokenInvariant(std::string message)
{
    Simulation::current()->brokenInvariant(std::move(message));
}

UInt64 advanceClock()
{
    return Simulation::current()->advanceClock();
}

}
