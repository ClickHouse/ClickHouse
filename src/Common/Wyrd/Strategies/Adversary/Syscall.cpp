#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Syscall.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Access.h>

namespace DB::Runtime::Wyrd
{

void enterThread()
{
    if (auto * current = adversary())
        current->threadEnter();
}

void exitThread()
{
    if (auto * current = adversary())
        current->threadExit();
}

void enterLockFreeScope()
{
    if (auto * current = adversary())
        current->threadEnterLockFreeScope();
}

void leaveLockFreeScope()
{
    if (auto * current = adversary())
        current->threadLeaveLockFreeScope();
}

void progressHint()
{
    if (auto * current = adversary())
        current->threadProgress();
}

}
