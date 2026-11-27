#pragma once

#include <Common/Wyrd/Facade/Atomic.h>
#include <base/types.h>

#include <source_location>

#if !defined(WYRD_ENABLED)
#    include <Common/futex.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value, std::source_location location = std::source_location::current());
void futexWakeOne(Atomic<UInt32> & address, std::source_location location = std::source_location::current());
void futexWakeAll(Atomic<UInt32> & address, std::source_location location = std::source_location::current());

}

namespace AdversaryMode
{

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value, std::source_location location = std::source_location::current());
void futexWakeOne(Atomic<UInt32> & address, std::source_location location = std::source_location::current());
void futexWakeAll(Atomic<UInt32> & address, std::source_location location = std::source_location::current());

}

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value, std::source_location location = std::source_location::current());
void futexWakeOne(Atomic<UInt32> & address, std::source_location location = std::source_location::current());
void futexWakeAll(Atomic<UInt32> & address, std::source_location location = std::source_location::current());

#else

using DB::futexWaitFetch;
using DB::futexWakeOne;
using DB::futexWakeAll;

#endif

}
