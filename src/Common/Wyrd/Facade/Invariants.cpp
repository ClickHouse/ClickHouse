#include <Common/Wyrd/Facade/Invariants.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <stdexcept>
#else
#    include <Common/Exception.h>
#endif

namespace DB
{

#if !defined(WYRD_ENABLED)
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
#endif

namespace Wyrd
{

void verify(bool condition, std::string message)
{
    if (!condition)
        abort(std::move(message));
}

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

static void abort(std::string message)
{
    brokenInvariant(std::move(message));
}

}

namespace AdversaryMode
{

static void abort(std::string message)
{
    throw std::runtime_error("Broken invariant: " + message);
}

}

void abort(std::string message)
{
    if (getMode() == Mode::Simulation)
        SimulationMode::abort(std::move(message));
    else
        AdversaryMode::abort(std::move(message));
}

#else

void abort(std::string message)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}", message);
}

#endif

}

}
