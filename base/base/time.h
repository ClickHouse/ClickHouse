#pragma once

#include <ctime>
#include <cstdlib>

/// `CLOCK_MONOTONIC_COARSE` is a Linux extension: it is only ever used where a cheaper,
/// less precise clock would do. Elsewhere (Darwin, SunOS, ...) fall back to the nearest
/// equivalent, or to the precise clock if there is none.
///
/// Emscripten is the awkward case: it *defines* the macro, so the fallback below would not
/// fire, but `clock_gettime` then returns EINVAL for it - which `clock_gettime_ns` turns into
/// a thrown `std::system_error` from every timing call. Override it explicitly.
#if defined (OS_WASM)
#    undef CLOCK_MONOTONIC_COARSE
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#elif !defined (CLOCK_MONOTONIC_COARSE)
#    if defined (OS_FREEBSD)
#        define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#    else
#        define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#    endif
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
