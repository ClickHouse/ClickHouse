#pragma once

#include <base/getThreadId.h>
#include <base/types.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <optional>

namespace DB
{

/// Diagnostics accumulator of one multi-way merge wave (`Aggregator::mergeBucketMultiWayImpl`
/// under `log_per_bucket_merge_timings`): the pooled tasks of
/// `UniqExactSet::parallelizeMergePrepare` / `parallelizeMergeMulti` - and of the pairwise pooled
/// merge the latter falls back to internally - accumulate their thread-CPU time and register
/// their worker thread here, so the wave's log line can report the effective wave width
/// (cpu/wall) and the number of distinct workers that served it.
struct MergeWaveStats
{
    /// Sum of the pooled tasks' thread-CPU time (CLOCK_THREAD_CPUTIME_ID deltas), nanoseconds.
    std::atomic<UInt64> cpu_ns{0};

    /// Worker thread ids, each registered once: only a thread itself inserts its own id, so its
    /// scan of the already-registered slots cannot miss it (its own earlier store is visible to
    /// it in program order), while concurrent registrations of other threads claim distinct slots
    /// through the counter. Bounded: a wave served by more distinct threads than there are slots
    /// saturates the count at the bound (with the pool sizes aggregation uses, that cannot
    /// happen).
    static constexpr size_t max_tracked_workers = 256;
    std::array<std::atomic<UInt64>, max_tracked_workers> worker_thread_ids{};
    std::atomic<size_t> num_workers{0};

    void registerWorker(UInt64 thread_id)
    {
        const size_t seen = std::min(num_workers.load(std::memory_order_acquire), max_tracked_workers);
        for (size_t i = 0; i < seen; ++i)
            if (worker_thread_ids[i].load(std::memory_order_relaxed) == thread_id)
                return;
        const size_t slot = num_workers.fetch_add(1, std::memory_order_acq_rel);
        if (slot < max_tracked_workers)
            worker_thread_ids[slot].store(thread_id, std::memory_order_relaxed);
    }

    size_t distinctWorkers() const { return std::min(num_workers.load(std::memory_order_relaxed), max_tracked_workers); }
};

/// The ambient per-thread sink of the wave running on this thread. The coordinating thread of a
/// wave sets it around the `parallelizeMergePrepare` + `parallelizeMergeMulti` calls (only when
/// `log_per_bucket_merge_timings` is on); the instrumented functions read it at entry - still on
/// the coordinating thread, so combinator forwarding keeps it visible - and capture the pointer
/// into their pooled tasks. Concurrent waves run on distinct coordinating threads and each sees
/// its own sink. A thread-local instead of a parameter, so the instrumentation does not ripple
/// through every `IAggregateFunction::parallelizeMerge*` override and combinator forwarder.
inline thread_local MergeWaveStats * current_merge_wave_stats = nullptr;

/// Sets the ambient sink for the duration of a scope, exception-safe: a wave that throws after
/// its barrier must not leave a dangling sink behind for the next wave on this thread.
class MergeWaveStatsScope
{
public:
    explicit MergeWaveStatsScope(MergeWaveStats * stats) { current_merge_wave_stats = stats; }
    ~MergeWaveStatsScope() { current_merge_wave_stats = nullptr; }

    MergeWaveStatsScope(const MergeWaveStatsScope &) = delete;
    MergeWaveStatsScope & operator=(const MergeWaveStatsScope &) = delete;
};

/// Measures the enclosing pooled task: its thread-CPU time and its worker's identity, recorded on
/// destruction (thus also when the task unwinds on exception or cancellation). No-op when the
/// wave carries no sink (the timing setting is off).
class MergeWaveTaskTimer
{
public:
    explicit MergeWaveTaskTimer(MergeWaveStats * stats_) : stats(stats_)
    {
        if (stats)
            cpu_watch.emplace(CLOCK_THREAD_CPUTIME_ID);
    }

    ~MergeWaveTaskTimer()
    {
        if (stats)
        {
            stats->cpu_ns.fetch_add(cpu_watch->elapsedNanoseconds(), std::memory_order_relaxed);
            stats->registerWorker(getThreadId());
        }
    }

    MergeWaveTaskTimer(const MergeWaveTaskTimer &) = delete;
    MergeWaveTaskTimer & operator=(const MergeWaveTaskTimer &) = delete;

private:
    MergeWaveStats * stats;
    std::optional<Stopwatch> cpu_watch;
};

}
