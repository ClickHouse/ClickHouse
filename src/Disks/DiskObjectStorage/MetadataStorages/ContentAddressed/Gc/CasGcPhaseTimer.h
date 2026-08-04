#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/types.h>
#include <map>
#include <utility>

namespace DB::Cas
{

/// Times ONE GC phase and emits a `GcPhaseRecord` through the round's sink on destruction.
///
/// The `ProfileEvents` delta is a plain snapshot DIFFERENCE of whatever counters container is currently
/// attached to this thread. It deliberately does NOT use a nested `ProfileEventsScope`: that RE-PARENTS
/// the thread's counters, and the round-level scope `CasGcScheduler::runRoundLogged` installs is already
/// holding that slot. A snapshot diff composes with the outer scope instead of fighting it, and degrades
/// to an empty map on a thread with no `ThreadStatus` (a bare gtest thread) -- the same degradation the
/// round-level capture already accepts and documents.
///
/// WHAT THE DELTA DOES NOT COVER: work this phase hands to another thread. `meta_pool_wait` is the one
/// phase where that is the whole content of the phase, so its row carries explicit `jobs_scheduled` /
/// `jobs_completed` metrics instead; see its instrumentation site in `Gc::runRegularRound`. Anywhere
/// else, a phase that shows a long duration and an empty delta means the time went somewhere this
/// instrumentation cannot see, and that is a finding rather than a blank to be ignored.
///
/// RAII AND FAILURE: the record is emitted from the destructor, so a phase that THREW still produces its
/// row while the stack unwinds. That is deliberate -- a round that failed is the round a reader most
/// needs, and it is why the correlator is `round_id` (which always exists) and not the round number
/// (which a failed round never obtains).
///
/// Cost per phase: one `Stopwatch` and two counters snapshots, against phases that each perform network
/// I/O. Always on; no setting, deliberately -- a knob whose default nobody remembers is how
/// instrumentation degrades to silence.
class GcPhaseTimer
{
public:
    /// `sink_` is `Gc::phase_sink`, which outlives every timer of the round (the scheduler clears it only
    /// after `runRegularRound` returns). `phase_` must be a string literal.
    GcPhaseTimer(const GcPhaseSink & sink_, const char * phase_)
        : sink(sink_), phase(phase_), attached(CurrentThread::isInitialized())
    {
        if (attached)
            before = CurrentThread::getProfileEvents().getPartiallyAtomicSnapshot();
    }

    GcPhaseTimer(const GcPhaseTimer &) = delete;
    GcPhaseTimer & operator=(const GcPhaseTimer &) = delete;

    /// Record one phase-specific count. Overwrites a previous value for the same key.
    void metric(const String & key, UInt64 value) { metrics[key] = value; }

    ~GcPhaseTimer()
    {
        if (!sink)
            return;
        GcPhaseRecord rec;
        rec.phase = phase;
        rec.duration_us = watch.elapsedMicroseconds();
        rec.metrics = std::move(metrics);
        if (attached)
        {
            const auto after = CurrentThread::getProfileEvents().getPartiallyAtomicSnapshot();
            for (ProfileEvents::Event e = ProfileEvents::Event(0); e < ProfileEvents::Counters::num_counters; ++e)
            {
                const auto delta = after[e] - before[e];
                if (delta != 0)
                    rec.profile_events.emplace(String(ProfileEvents::getName(e)), static_cast<UInt64>(delta));
            }
        }
        /// Best-effort, exactly like the round-row sink: instrumentation must never break GC, and this
        /// runs in a destructor that may already be unwinding a round exception.
        try { sink(rec); } catch (...) {}   // NOLINT(bugprone-empty-catch)
    }

private:
    const GcPhaseSink & sink;
    const char * phase;
    bool attached;
    Stopwatch watch{CLOCK_MONOTONIC};
    ProfileEvents::Counters::Snapshot before;
    std::map<String, UInt64> metrics;
};

}
