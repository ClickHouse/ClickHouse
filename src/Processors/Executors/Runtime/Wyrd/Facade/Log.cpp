#include <Processors/Executors/Runtime/Wyrd/Facade/Log.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#else
#    include <base/getThreadId.h>

#    include <cstdio>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

void logMessage(std::string message)
{
    trace(std::move(message));
}

#else

void logMessage(std::string message)
{
    fmt::print(stderr, "Wyrd thread {}: {}\n", getThreadId(), message);
}

#endif

}
