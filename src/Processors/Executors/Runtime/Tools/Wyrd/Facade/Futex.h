#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Config.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Atomic.h>
#include <base/types.h>

#if !defined(EXECUTOR_SIM) && !defined(EXECUTOR_FAULT_INJECTION)
#    include <Common/futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(EXECUTOR_SIM) || defined(EXECUTOR_FAULT_INJECTION)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value);
void futexWakeOne(Atomic<UInt32> & address);
void futexWakeAll(Atomic<UInt32> & address);

#else

using DB::futexWaitFetch;
using DB::futexWakeOne;
using DB::futexWakeAll;

#endif

}
