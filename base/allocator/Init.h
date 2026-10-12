#pragma once

/// Initialization of the allocator and fork handling (jemalloc: the initialization functions of `src/jemalloc.c` and
/// `jemalloc_prefork` / `jemalloc_postfork_*`).
///
/// The initialization state, `mallocInitialized`, `mallocInit` and `mallocInitHard` are declared in Frontend.h (the
/// fast paths need them), `mallocInitA0` in Arenas.h (the bootstrap allocations need it), `num_cpus` in Mutex.h.

#include <allocator/Common.h>

namespace jemalloc
{

/// The number of CPUs in the affinity mask of the process (`sysconf(_SC_NPROCESSORS_ONLN)` on Darwin); 1 if it cannot
/// be determined. No cgroup quota awareness.
/// jemalloc: malloc_ncpus
unsigned mallocNumCPUs();

/// Whether the number of CPUs is the same based on the affinity mask, `_SC_NPROCESSORS_ONLN` and
/// `_SC_NPROCESSORS_CONF` (otherwise per-CPU arenas are disabled).
/// jemalloc: malloc_cpu_count_is_deterministic
bool mallocCPUCountIsDeterministic();

/// Acquire all mutexes in a safe order before `fork` / release them after it (Fork.cpp). Registered with
/// `pthread_atfork` on Linux (except ppc64le); on FreeBSD libc calls the exported `_malloc_prefork` /
/// `_malloc_postfork`; on Darwin the zone's `force_lock` / `force_unlock` call them.
/// jemalloc: jemalloc_prefork, jemalloc_postfork_parent, jemalloc_postfork_child
void jemallocPrefork();
void jemallocPostforkParent();
void jemallocPostforkChild();

}
