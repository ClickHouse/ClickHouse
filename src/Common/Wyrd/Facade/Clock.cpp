#include <Common/Wyrd/Facade/Clock.h>

#if defined(WYRD_SIMULATION)
#    include <Common/Wyrd/Random.h>
#else
#    include <Common/Stopwatch.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_SIMULATION)

UInt64 monotonicNanoseconds()
{
    thread_local UInt64 clock_ns = 0;
    clock_ns += 1'000 + randomNumber() % 9'000;
    return clock_ns;
}

#elif defined(WYRD_ADVERSARY)

UInt64 monotonicNanoseconds()
{
    return clock_gettime_ns(CLOCK_MONOTONIC);
}

#else

UInt64 monotonicNanoseconds()
{
    return clock_gettime_ns(CLOCK_MONOTONIC);
}

#endif

}
