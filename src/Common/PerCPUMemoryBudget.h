#pragma once

#include <base/types.h>

/// Per-CPU memory budget that mirrors `ThreadStatus::untracked_memory` on a
/// per-CPU rather than per-thread axis. Every allocation does both:
///   * `untracked_memory += size`            — per-thread (thread-local)
///   * `PerCPUMemoryBudget::charge(size)`    — per-CPU rseq RMW/atomic
/// Frees mirror with negative `delta`
///
/// At first use we charge total_memory_tracker with `SLICE * effective_cpus`
/// once and seed each slice with `SLICE`. The reservation is never released
/// — it acts as the buffer within which the slices oscillate. The total_memory_tracker
/// therefore stays an upper bound on real memory usage even when `Σ untracked`
/// across all threads on the system grows.
namespace DB::PerCPUMemoryBudget
{

constexpr Int64 SLICE = 4 * 1024 * 1024;

bool isReady();
bool isRSeqReady();

/// Use:
///   alloc:  charge(+size)
///   free:   charge(-size)
///
/// Returns true when the slice has drifted out of `[0, 2 * SLICE]` — the
/// caller flushes its own per-thread `untracked_memory` to the tracker
/// chain (which counter-charges the slice back toward `SLICE`):
///   * slice < 0          — alloc-heavy: pending allocs exceed reservation
///   * slice > 2 * SLICE  — free-heavy: pending frees exceed reservation
bool charge(Int64 delta);

/// Σ slice values across all CPUs — diagnostic for `system.asynchronous_metrics`.
Int64 reservedBytes();

}
