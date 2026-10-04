#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <base/types.h>

#if !defined(WYRD_SIMULATION) && !defined(WYRD_ADVERSARY)
#    include <Common/futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value);
void futexWakeOne(Atomic<UInt32> & address);
void futexWakeAll(Atomic<UInt32> & address);

#else

using DB::futexWaitFetch;
using DB::futexWakeOne;
using DB::futexWakeAll;

#endif

}
