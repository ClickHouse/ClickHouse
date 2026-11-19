#include <Common/Wyrd/Strategies/Adversary/Syscall.h>
#include <Common/Wyrd/Strategies/Adversary/Access.h>

#include <utility>

namespace DB::Wyrd
{

void enterThread()
{
    if (auto * current = adversary())
        current->threadEnter();
}

void failThread(std::exception_ptr exception)
{
    if (auto * current = adversary())
        current->threadFail(std::move(exception));
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
