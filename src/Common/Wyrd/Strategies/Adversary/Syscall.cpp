#include <Common/Wyrd/Strategies/Adversary/Syscall.h>
#include <Common/Wyrd/Strategies/Adversary/Access.h>

#include <utility>

namespace DB::Wyrd
{

void enterThread()
{
    adversary()->threadEnter();
}

void failThread(std::exception_ptr exception)
{
    adversary()->threadFail(std::move(exception));
}

void exitThread()
{
    adversary()->threadExit();
}

void enterLockFreeScope()
{
    adversary()->threadEnterLockFreeScope();
}

void leaveLockFreeScope()
{
    adversary()->threadLeaveLockFreeScope();
}

void progressHint()
{
    adversary()->threadProgress();
}

}
