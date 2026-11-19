#include <Common/Wyrd/Facade/Log.h>

#if defined(WYRD_SIMULATION)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <base/getThreadId.h>
#    include <cstdio>
#else
#    include <base/getThreadId.h>
#    include <cstdio>
#endif

namespace WYRD_NAMESPACE(DB::Wyrd)
{

#if defined(WYRD_SIMULATION)

void logMessage(std::string message)
{
    trace(std::move(message));
}

#elif defined(WYRD_ADVERSARY)

void logMessage(std::string)
{
}

#else

void logMessage(std::string)
{
}

#endif

}
