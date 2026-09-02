#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <base/getThreadId.h>
#include <base/types.h>

#include <algorithm>
#include <array>
#include <atomic>

namespace ProfileEvents
{
extern const Event UniqExactMergeWaves;
extern const Event UniqExactMergeWaveInputStates;
extern const Event UniqExactMergeWaveElapsedMicroseconds;
extern const Event UniqExactMergeWaveCPUTimeMicroseconds;
extern const Event UniqExactMergeWaveWorkers;
}

namespace DB
{

/// Accumulates one existing uniqExact thread-pool dispatch. The coordinating query thread owns
/// the object and reports it after the completion barrier; pooled tasks only add their thread CPU
/// time and register the worker that executed them.
class UniqExactMergeWaveStats
{
public:
    explicit UniqExactMergeWaveStats(size_t input_states_)
        : input_states(input_states_)
    {
    }

    void recordTask(UInt64 cpu_nanoseconds) noexcept
    {
        cpu_time_nanoseconds.fetch_add(cpu_nanoseconds, std::memory_order_relaxed);
        registerWorker(getThreadId());
    }

    void report() const
    {
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaves);
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaveInputStates, input_states);
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaveElapsedMicroseconds, wall_watch.elapsedMicroseconds());
        ProfileEvents::increment(
            ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds, cpu_time_nanoseconds.load(std::memory_order_relaxed) / 1000);
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaveWorkers, distinctWorkers());
    }

private:
    static constexpr size_t max_tracked_workers = 256;

    void registerWorker(UInt64 thread_id) noexcept
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

    size_t input_states;
    Stopwatch wall_watch;
    std::atomic<UInt64> cpu_time_nanoseconds = 0;
    std::array<std::atomic<UInt64>, max_tracked_workers> worker_thread_ids{};
    std::atomic<size_t> num_workers = 0;
};

/// Measures one pooled task with the per-thread CPU clock. The stats object outlives every task
/// because callers report only after their thread-pool completion barrier.
class UniqExactMergeWaveTaskTimer
{
public:
    explicit UniqExactMergeWaveTaskTimer(UniqExactMergeWaveStats & stats_)
        : stats(stats_)
    {
        cpu_clock_available = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_start) == 0;
    }

    ~UniqExactMergeWaveTaskTimer() noexcept
    {
        timespec cpu_end{};
        if (!cpu_clock_available || clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end) != 0)
            return;

        const UInt64 start_nanoseconds = static_cast<UInt64>(cpu_start.tv_sec) * 1'000'000'000 + cpu_start.tv_nsec;
        const UInt64 end_nanoseconds = static_cast<UInt64>(cpu_end.tv_sec) * 1'000'000'000 + cpu_end.tv_nsec;
        if (end_nanoseconds >= start_nanoseconds)
            stats.recordTask(end_nanoseconds - start_nanoseconds);
    }

    UniqExactMergeWaveTaskTimer(const UniqExactMergeWaveTaskTimer &) = delete;
    UniqExactMergeWaveTaskTimer & operator=(const UniqExactMergeWaveTaskTimer &) = delete;

private:
    UniqExactMergeWaveStats & stats;
    timespec cpu_start{};
    bool cpu_clock_available = false;
};

}
