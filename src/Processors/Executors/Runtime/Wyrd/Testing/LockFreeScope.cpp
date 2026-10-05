#include <Processors/Executors/Runtime/Wyrd/Testing/LockFreeScope.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/Access.h>
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
    if (auto * current = adversary())
        current->threadEnterLockFreeScope();
}

LockFreeScope::~LockFreeScope()
{
    if (auto * current = adversary())
        current->threadLeaveLockFreeScope();
}

void progress()
{
    if (auto * current = adversary())
        current->threadProgress();
}

#else

LockFreeScope::LockFreeScope() = default;

LockFreeScope::~LockFreeScope() = default;

void progress()
{
}

#endif

}
