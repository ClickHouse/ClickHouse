#pragma once

#include <time.h>

#if defined (OS_DARWIN) || defined (OS_SUNOS)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#elif defined (OS_FREEBSD)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#endif
