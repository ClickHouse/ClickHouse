#pragma once

#include <ctime>

/// `CLOCK_MONOTONIC_COARSE` is a Linux extension: same epoch as `CLOCK_MONOTONIC` but read
/// straight from the last timer tick, so it is cheaper and only millisecond-accurate. Where it
/// does not exist, fall back to the exact clock - callers ask for it to save time, never for
/// its lower resolution. mingw-w64 (winpthreads) offers `CLOCK_REALTIME_COARSE` but no
/// monotonic counterpart, and a coarse wall clock is not a substitute: it is not monotonic.
#if defined (OS_DARWIN) || defined (OS_SUNOS) || defined (OS_WINDOWS)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#elif defined (OS_FREEBSD)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#endif

#if defined(OS_WINDOWS)
/// `timegm` is the inverse of `gmtime`: it interprets a broken-down time as UTC, where `mktime`
/// interprets it as local time and so depends on the current time zone. The Windows CRT provides
/// exactly this under its own name.
inline std::time_t timegm(std::tm * tm)
{
    return ::_mkgmtime(tm);
}
#endif
