#include "Memory.h"

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>

bool enableBackgroundMemoryPurge()
{
    bool value = true;
    // background threads for purging memory asynchronously will help with the
    // memory not freed by inactive threads, since ThreadPool select job
    // randomly there can be some threads that had been performed some memory
    // heavy task and will be inactive for some time, and until it will became
    // active again, the memory will not be freed (note that default number of
    // areans is 4*CPU).
    //
    // There are the following metrics in the system.asynchronous_metrics that
    // will show how active threads are:
    // - jemalloc.background_thread.num_runs
    // - jemalloc.background_thread.run_interval
    //
    // It is enabled for server only (and not via JEMALLOC_CONFIG_MALLOC_CONF
    // in jemalloc-cmake/**/jemalloc_internal_defs.h), since this is threads
    // and other utils are not that hot to requires this.
    if (mallctl("background_thread", nullptr, nullptr, reinterpret_cast<void *>(&value), sizeof(value)))
        return false;

    size_t size = sizeof(value);
    value = false;
    if (mallctl("background_thread", reinterpret_cast<void *>(&value), &size, nullptr, 0))
        return false;

    return value;
}

#else
bool enableBackgroundMemoryPurge() { return false; }
#endif
