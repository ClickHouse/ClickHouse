#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <base/types.h>

#include <source_location>

#if !defined(WYRD_SIMULATION) && !defined(WYRD_ADVERSARY)
#    include <Common/futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value, std::source_location location = std::source_location::current());
void futexWakeOne(Atomic<UInt32> & address, std::source_location location = std::source_location::current());
void futexWakeAll(Atomic<UInt32> & address, std::source_location location = std::source_location::current());

#else

using DB::futexWaitFetch;
using DB::futexWakeOne;
using DB::futexWakeAll;

#endif

}
