#pragma once

#include <ctime>

#if defined (OS_DARWIN) || defined (OS_SUNOS)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#elif defined (OS_FREEBSD)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#endif

/// `CLOCK_MONOTONIC_COARSE` is a Linux extension. Fall back to the precise clock everywhere else
/// (WebAssembly, for instance) - it is only ever used where a cheaper, less precise clock would do.
#if !defined (CLOCK_MONOTONIC_COARSE)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif
