#pragma once

#include <time.h>

#if defined (OS_DARWIN)
#    define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif
