#pragma once

#include <base/types.h>


namespace DB
{

/// Speculatively reserves `additional_memory_tracking_per_thread` on the server-wide
/// memory tracker for the lifetime of a pipeline worker. Each thread accumulates up to
/// `max_untracked_memory` of allocations before reporting them to its tracker chain,
/// so with many threads the server's tracked memory under-counts actual consumption;
/// the reservation restores a safe upper bound at the server level.
///
/// The reservation is deliberately charged on the total (server-wide) tracker only and
/// not through the current thread's tracker chain: query-level and user-level accounting
/// must stay exact, because heuristics compare query memory deltas against byte
/// thresholds - e.g. the `Aggregator` conversion to two-level hash tables
/// (`group_by_two_level_threshold_bytes`) and spill-to-disk decisions. Phantom
/// reservations of `num_threads * 4 MiB` would trip those thresholds immediately and
/// were observed as slowdowns of small GROUP BY queries in performance tests.
///
/// All guards alive on one thread share a single reservation, because they also share
/// that thread's single untracked-memory buffer. The reservation is reference-counted
/// rather than owned by the outermost guard: guard lifetimes are not always nested. A
/// nested executor running inside a fiber (e.g. `Connection::sendExternalTablesData`
/// called from `RemoteSource`) can be suspended with its guard alive while the outer
/// guard is destroyed, and that fiber can later be resumed and finished on another
/// thread. The charge is released only when the last guard sharing it is destroyed, so
/// a live guard always implies a live charge for the thread that took it.
///
/// Sharing is limited to guards of the same query (the same credited query tracker): a
/// pooled thread can run a guard of another query while a suspended guard of the previous
/// one is still alive, and the new guard then takes its own charge. The thread keeps every
/// live charge discoverable, so when the previous query returns to the thread while its
/// suspended guard is alive, the new guard shares that charge instead of taking another.
/// Each charge keeps the thread group of its query alive, so the credited tracker outlives
/// the last guard.
///
/// The constructor may throw `MEMORY_LIMIT_EXCEEDED` (the throwing path is intentional);
/// it must be invoked inside the worker's `try` block so the error propagates through
/// the pipeline like any other job failure.
///
/// The destructor flushes the current thread's untracked memory before dropping its
/// reference, so the server-wide tracker never loses both the speculative charge and
/// the thread's deferred allocations, including on an exception.
class SpeculativeMemoryReservation
{
public:
    SpeculativeMemoryReservation();
    ~SpeculativeMemoryReservation();

    SpeculativeMemoryReservation(const SpeculativeMemoryReservation &) = delete;
    SpeculativeMemoryReservation & operator=(const SpeculativeMemoryReservation &) = delete;

    /// The amount charged by the reservation this guard shares, or zero if the guard holds none
    /// (the setting was zero when the first guard on the thread was created).
    Int64 getSize() const;

    struct State;

private:
    State * state = nullptr;
};

}
