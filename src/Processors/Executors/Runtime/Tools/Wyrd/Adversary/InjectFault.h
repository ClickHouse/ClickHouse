#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Config.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/Access.h>

namespace DB::Runtime::Wyrd
{

inline void injectFault()
{
    if (auto * current = adversary())
        current->threadFault();
}

inline void injectFaultBefore()
{
#if defined(EXECUTOR_FAULT_BEFORE)
    injectFault();
#endif
}

inline void injectFaultAfter()
{
#if defined(EXECUTOR_FAULT_AFTER)
    injectFault();
#endif
}

inline bool spuriousFailure()
{
    if (auto * current = adversary())
        return current->threadSpuriousFailure();
    return false;
}

}
