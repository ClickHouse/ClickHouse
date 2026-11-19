#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <stdexcept>
#else
#    include <Common/Exception.h>
#endif

namespace DB
{

#if !defined(WYRD_SIMULATION) && !defined(WYRD_ADVERSARY)
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
#endif

namespace Runtime::Wyrd
{

void verify(bool condition, std::string message)
{
    if (!condition)
        abort(std::move(message));
}

#if defined(WYRD_SIMULATION)

void abort(std::string message)
{
    brokenInvariant(std::move(message));
}

#elif defined(WYRD_ADVERSARY)

void abort(std::string message)
{
    throw std::runtime_error("Broken invariant: " + message);
}

#else

void abort(std::string message)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}", message);
}

#endif

}

}
