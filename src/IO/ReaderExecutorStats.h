#pragma once

#include <Common/Stopwatch.h>
#include <base/types.h>

#include <array>

namespace DB
{

/// Per-executor accumulating stats, flushed to ProfileEvents as they
/// happen and logged at destruction. The foreground passes `this->stats`
/// into the read path; a worker passes the machine's own `Stats` (merged
/// at collect/cancel), so a worker never writes a shared counter.
struct ReaderExecutorStats
{
    /// `add` is the only mutator and the single place a counter maps to
    /// its ProfileEvent, so the two can never drift apart.
    enum Counter : size_t
    {
        BytesFromPageCache,
        BytesFromFilesystemCache,
        BytesFromSource,
        BytesPushedToCacheSync,
        BytesPromoted,
        CacheGetRequests,
        CachePopulateRequests,
        SourceRequests,
        /// Source connections dropped before their right bound (not
        /// pool-reusable; the metric's `I`).
        IncompleteConnections,
        /// Useful bytes delivered to read requests (cost denominator).
        RequestedBytes,
        CacheGetMicroseconds,
        CachePopulateMicroseconds,
        SourceReadMicroseconds,
        DecryptMicroseconds,
        PrefetchWaitMicroseconds,
        WorkMicroseconds,
        PrefetchHits,
        PrefetchCancelled,
        PrefetchPoolFull,
        PrefetchDiscardedRunning,
        PrefetchIssuedSourceBytes,
        PrefetchWastedSourceBytes,
        /// A machine wrapped up early at an interrupt point on request.
        MachineInterrupted,
        /// Collects that served a non-empty partial prefix of an
        /// interrupted fetch.
        PartialCollects,
        /// A deferred cache fill whose put step threw - logged, never the
        /// client's error.
        PutFailed,
        /// Long source connections: opened, windows served from an open one,
        /// fallbacks to a one-shot when no slot was free, bytes served through them.
        LongConnectionOpened,
        LongConnectionHits,
        LongConnectionFallbacks,
        LongConnectionBytes,
        /// Number of `observeAndSchedule` calls = residency-plan (re)builds. The
        /// plan is reused across mark-range advances; it rebuilds only on a
        /// want_replan (the cursor leaves `plan_start..plan_end`). This sizes how
        /// short-lived the held cache readers are.
        Observations,
        /// Number of `extendPlan` calls: the plan grew forward in place - held
        /// buffers kept, only the new span observed - where an epoch rebuild
        /// (an Observation) would have re-probed everything.
        PlanExtensions,
        NumCounters
    };

    /// Bump `c` AND emit its ProfileEvent (the one place events are
    /// incremented, so a worker in the submitter's thread group attributes
    /// to the query too).
    void add(Counter c, UInt64 value = 1);

    /// Read a counter for the final report; does not emit.
    UInt64 get(Counter c) const { return values[c]; }

    /// Roll another executor's / worker's stats into this aggregate
    /// WITHOUT re-emitting (each counter already hit ProfileEvents at its
    /// `add`).
    ReaderExecutorStats & operator+=(const ReaderExecutorStats & o);

private:
    std::array<UInt64, NumCounters> values{};
};

/// RAII timer: on scope exit, add the elapsed microseconds to a stats
/// timing counter through `ReaderExecutorStats::add`.
class ReaderExecutorStatTimer
{
public:
    ReaderExecutorStatTimer(ReaderExecutorStats & stats_, ReaderExecutorStats::Counter counter_);
    ~ReaderExecutorStatTimer();

    ReaderExecutorStatTimer(const ReaderExecutorStatTimer &) = delete;
    ReaderExecutorStatTimer & operator=(const ReaderExecutorStatTimer &) = delete;

    UInt64 elapsedMicroseconds() const { return watch.elapsedMicroseconds(); }

private:
    ReaderExecutorStats & target;
    ReaderExecutorStats::Counter counter;
    Stopwatch watch;
};

}
