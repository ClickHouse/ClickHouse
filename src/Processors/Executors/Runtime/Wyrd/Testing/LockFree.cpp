#include <Processors/Executors/Runtime/Wyrd/Testing/LockFree.h>

#if defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/Access.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_ADVERSARY)

LockFreeScope::LockFreeScope()
{
    if (auto * current = adversary())
        current->threadSetLockFree(true);
}

LockFreeScope::~LockFreeScope()
{
    if (auto * current = adversary())
        current->threadSetLockFree(false);
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
