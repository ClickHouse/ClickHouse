#pragma once

#include <ctime>

/// `CLOCK_MONOTONIC_COARSE` is a Linux extension: it is only ever used where a cheaper,
/// less precise clock would do. Elsewhere (Darwin, SunOS, WebAssembly, ...) fall back to the
/// nearest equivalent, or to the precise clock if there is none.
#if !defined (CLOCK_MONOTONIC_COARSE)
#    if defined (OS_FREEBSD)
#        define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#    else
#        define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#    endif
#endif
