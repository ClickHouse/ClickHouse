#include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/Simulator.h>

namespace DB::Runtime::Wyrd
{

void sync(const char * operation, std::source_location location)
{
    if (auto * simulator = Simulator::current())
        simulator->sync(operation, location);
}

void yield(std::source_location location)
{
    if (auto * simulator = Simulator::current())
        simulator->yield(location);
}

void futexWait(void * address, std::source_location location)
{
    if (auto * simulator = Simulator::current())
        simulator->futexWait(address, location);
}

void futexWake(void * address, size_t count, std::source_location location)
{
    if (auto * simulator = Simulator::current())
        simulator->futexWake(address, count, location);
}

void spawn(std::function<void()> body, std::source_location location)
{
    if (auto * simulator = Simulator::current())
        simulator->spawn(std::move(body), location);
}

}
