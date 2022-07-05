#pragma once
#if defined(__aarch64__)
#    include "aarch64.h"
#elif defined(__x86_64__)
#    include "x86_64.h"
#else
#    include "unspecified.h"
#endif
