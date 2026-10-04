#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/InjectFault.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/Access.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Config.h>

namespace DB::Runtime::Wyrd
{

void injectFault()
{
    if (auto * current = adversary())
        current->threadFault();
}

void injectFaultBefore(const char *, std::source_location)
{
#if defined(EXECUTOR_FAULT_BEFORE)
    injectFault();
#endif
}

void injectFaultAfter(const char *, std::source_location)
{
#if defined(EXECUTOR_FAULT_AFTER)
    injectFault();
#endif
}

bool spuriousFailure(std::source_location)
{
    if (auto * current = adversary())
        return current->threadSpuriousFailure();
    return false;
}

}
