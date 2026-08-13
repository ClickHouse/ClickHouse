#pragma once

#include <ctime>

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
