#include <allocator/Frontend.h>
#include <allocator/Mutex.h>
#include <allocator/ThreadState.h>

/// The global state of the initialization that the lower layers read. It is separate from Init.cpp so that the unit
/// tests of those layers (which link the internals as a static library) do not pull in the whole boot sequence.

namespace jemalloc
{

/// jemalloc: malloc_init_state
constinit MallocInitState malloc_init_state = malloc_init_uninitialized;

/// False should be the common case. Set to true to trigger initialization. jemalloc: malloc_slow
constinit bool malloc_slow = true;

/// The number of CPUs (declared in Mutex.h). jemalloc: ncpus
constinit unsigned num_cpus = 0;

/// Declared in Frontend.h (here rather than in API.cpp because the `stats.zero_reallocs` mallctl reads it).
/// jemalloc: zero_realloc_count
constinit std::atomic<size_t> zero_realloc_count{0};

}
