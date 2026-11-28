#include <Common/Wyrd/Testing/LockFreeScope.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Adversary/Syscall.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

static void enterScope()
{
    enterLockFreeScope();
}

static void leaveScope()
{
    leaveLockFreeScope();
}

static void progress()
{
    progressHint();
}

}

namespace AdversaryMode
{

static void enterScope()
{
    enterLockFreeScope();
}

static void leaveScope()
{
    leaveLockFreeScope();
}

static void progress()
{
    progressHint();
}

}

LockFreeScope::LockFreeScope()
{
    if (getMode() == Mode::Simulation)
        SimulationMode::enterScope();
    else
        AdversaryMode::enterScope();
}

LockFreeScope::~LockFreeScope()
{
    if (getMode() == Mode::Simulation)
        SimulationMode::leaveScope();
    else
        AdversaryMode::leaveScope();
}

void progress()
{
    if (getMode() == Mode::Simulation)
        SimulationMode::progress();
    else
        AdversaryMode::progress();
}

#else

LockFreeScope::LockFreeScope() = default;

LockFreeScope::~LockFreeScope() = default;

void progress()
{
}

#endif

}
