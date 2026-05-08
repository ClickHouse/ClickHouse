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
/// `charge()` keeps the slice strictly inside `[0, 2 * SLICE]` — it refuses
/// to apply `delta` if doing so would cross either bound, and returns true
/// in that case. The caller treats the return value as "flush needed":
///   * if `delta` was applied (return false), the slice is in bounds and
///     no flush is required;
///   * if `delta` was refused (return true), the caller flushes its
///     `untracked_memory` to the tracker chain. Because `delta` (the
///     current op's size) was *not* subtracted from the slice, only the
///     previously-accumulated portion `untracked_memory - delta` is
///     counter-charged back to the slice; the full `untracked_memory` is
///     applied to `total_memory_tracker`.
///
/// Refusing instead of overcommitting prevents slices from getting stuck
/// far OOB and turning every subsequent op on that CPU into a forced
/// flush.
bool charge(Int64 delta);

/// Σ slice values across all CPUs — diagnostic for `system.asynchronous_metrics`.
Int64 reservedBytes();

}
