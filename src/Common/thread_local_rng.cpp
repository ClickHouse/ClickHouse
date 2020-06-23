#include "src/Common/thread_local_rng.h"
#include "src/Common/randomSeed.h"

thread_local pcg64 thread_local_rng{randomSeed()};
