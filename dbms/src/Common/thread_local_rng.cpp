#include "thread_local_rng.h"

#include <Common/randomSeed.h>

#if defined(ARCADIA_BUILD)
thread_local SimpleRng64 thread_local_rng(randomSeed());
#else
thread_local pcg64 thread_local_rng{randomSeed()};
#endif
