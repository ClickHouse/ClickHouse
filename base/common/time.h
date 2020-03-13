#pragma once

#include <time.h>

#if defined (OS_DARWIN) or defined (OS_FREEBSD)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif
