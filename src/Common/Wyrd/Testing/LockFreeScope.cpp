#include <Common/Wyrd/Testing/LockFreeScope.h>

#if defined(WYRD_SIMULATION)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <Common/Wyrd/Strategies/Adversary/Syscall.h>
#endif

namespace WYRD_NAMESPACE(DB::Wyrd)
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
