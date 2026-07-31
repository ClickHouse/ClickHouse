#pragma once

#include <ctime>
#include <cstdlib>

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
/// `setenv` for the one variable this codebase sets, `TZ`. The Windows CRT spells it `_putenv_s`,
/// which always overwrites - so this only accepts the `overwrite = 1` that every caller passes.
inline int setenv(const char * name, const char * value, int overwrite)
{
    if (!overwrite)
        return 0;
    return ::_putenv_s(name, value);
}

/// `localtime_r` is the reentrant `localtime`. The Windows CRT spells it `localtime_s`, with the
/// arguments the other way round and an `errno_t` return instead of a pointer.
inline std::tm * localtime_r(const std::time_t * time, std::tm * result)
{
    return ::localtime_s(result, time) == 0 ? result : nullptr;
}

/// `timegm` is the inverse of `gmtime`: it interprets a broken-down time as UTC, where `mktime`
/// interprets it as local time and so depends on the current time zone. The Windows CRT provides
/// exactly this under its own name.
inline std::time_t timegm(std::tm * tm)
{
    return ::_mkgmtime(tm);
}
#endif
