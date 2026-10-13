#include <Processors/Executors/Runtime/Wyrd/Testing/LockFreeScope.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Syscall.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

LockFreeScope::LockFreeScope()
{
    enterLockFreeScope();
}

LockFreeScope::~LockFreeScope()
{
    leaveLockFreeScope();
}

void progress()
{
    progressHint();
}

#elif defined(WYRD_ADVERSARY)

LockFreeScope::LockFreeScope()
{
    enterLockFreeScope();
}

LockFreeScope::~LockFreeScope()
{
    leaveLockFreeScope();
}

void progress()
{
    progressHint();
}

#else

LockFreeScope::LockFreeScope() = default;

LockFreeScope::~LockFreeScope() = default;

void progress()
{
}

#endif

}
