#pragma once

#include <cstdint>
#include <unordered_set>

namespace DB
{

/** Process-wide registry of pthread stack base addresses.
  *
  * Registration is tied to OS thread lifecycle, not to any C++ object: each
  * thread on first call to `ensureCurrentThreadRegistered` queries its own
  * pthread stack base via pthread_getattr_np and registers it, and the
  * thread-local cleanup fires when the OS thread exits (via the standard
  * __cxa_thread_atexit hook for thread_local destructors).
  *
  * Tying registration to OS thread lifecycle avoids the trap of tying it to
  * the lifetime of a particular C++ holder (e.g. ThreadStatus, which can be
  * held in long-lived shared_ptrs and outlive the OS thread that constructed
  * it). Each live OS thread contributes exactly one entry while it is alive.
  *
  * AsynchronousMetrics takes a snapshot of the registry once per scrape and
  * matches each /proc/self/smaps VMA's start address against the snapshot to
  * attribute resident bytes to thread stacks. The snapshot is essential
  * because /proc/self/smaps iteration from inside a busy process sees many
  * ephemeral VMAs (the kernel releases mmap_lock between successive read()
  * syscalls; jemalloc chunks created in flight appear as extra records). By
  * matching against the snapshot taken before the walk starts, the result is
  * bounded by the number of threads at snapshot time, regardless of churn.
  */
class ThreadStackRegistry
{
public:
    /// Idempotent: registers the current OS thread's stack base on first call,
    /// arranges for automatic removal when the OS thread exits. No-op on
    /// subsequent calls from the same thread. Safe to call from any context.
    static void ensureCurrentThreadRegistered();

    /// A snapshot of the currently-registered stack base addresses.
    /// One allocation per call. Used at scrape time only.
    static std::unordered_set<uintptr_t> snapshot();
};

}
