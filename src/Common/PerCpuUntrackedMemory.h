#pragma once

#include <base/types.h>

/// Per-CPU untracked memory counters.
///
/// Replacement for the per-thread `untracked_memory` accumulator used by
/// `CurrentMemoryTracker`. One Int64 slot per CPU instead of per thread, so
/// the worst-case unflushed window scales with `ncpu` rather than `nthreads`.
///
/// This namespace is intentionally minimal: it owns the slot array and
/// nothing else. All overflow/limit policy stays in the caller — the per-slot
/// threshold is the same `untracked_memory_limit` the legacy path uses.
///
/// All RMW operations take an explicit CPU index. The caller resolves it
/// once (typically via `sched_getcpu()`) and reuses the same value for paired
/// add/drain. There is deliberately no `drainCurrent()` helper, because the
/// current CPU can change between two calls and silent re-resolution would
/// race overflow against migration.
namespace DB::PerCpuUntrackedMemory
{

/// True iff initialization succeeded (Linux + mmap of slot array OK).
/// Callers must check this before invoking add/drain/peekTotal.
bool isEnabled();

/// Number of slots, equal to `sysconf(_SC_NPROCESSORS_CONF)` at startup.
/// Fixed for the lifetime of the process.
int cpuCount();

/// slots[cpu] += delta; returns the new value of the slot.
Int64 add(int cpu, Int64 delta);

/// Atomically exchanges slots[cpu] with 0; returns the prior value.
Int64 drain(int cpu);

/// Sum of slots[i] across all i, with relaxed loads and no reset. O(ncpu).
/// Slow path — intended for `MemoryTracker` to consult under memory pressure.
Int64 peekTotal();

}
