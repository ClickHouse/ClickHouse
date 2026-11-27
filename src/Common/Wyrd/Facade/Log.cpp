#include <Common/Wyrd/Facade/Log.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <base/getThreadId.h>
#    include <cstdio>
#else
#    include <base/getThreadId.h>
#    include <cstdio>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

static void logMessage(std::string message)
{
    trace(std::move(message));
}

}

namespace AdversaryMode
{

static void logMessage(std::string)
{
}

}

void logMessage(std::string message)
{
    if (getMode() == Mode::Simulation)
        SimulationMode::logMessage(std::move(message));
    else
        AdversaryMode::logMessage(std::move(message));
}

#else

void logMessage(std::string)
{
}

#endif

}
