#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>

#include <base/getThreadId.h>
#include <base/types.h>

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
    UniqExactMergeWaveStats(size_t input_states_, size_t max_worker_tasks)
        : input_states(input_states_)
        , worker_thread_ids(max_worker_tasks)
    {
        for (auto & thread_id : worker_thread_ids)
            thread_id.store(0, std::memory_order_relaxed);
    }

    void recordTask(UInt64 cpu_nanoseconds, size_t processed_items_) noexcept
    {
        if (processed_items_ == 0)
            return;

        cpu_time_nanoseconds.fetch_add(cpu_nanoseconds, std::memory_order_relaxed);
        processed_items.fetch_add(processed_items_, std::memory_order_relaxed);
        registerWorker(getThreadId());
    }

    void report() const
    {
        if (processed_items.load(std::memory_order_relaxed) == 0)
            return;

        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaves);
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaveInputStates, input_states);
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaveElapsedMicroseconds, wall_watch.elapsedMicroseconds());
        ProfileEvents::increment(
            ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds, cpu_time_nanoseconds.load(std::memory_order_relaxed) / 1000);
        ProfileEvents::increment(ProfileEvents::UniqExactMergeWaveWorkers, distinctWorkers());
    }

private:
    void registerWorker(UInt64 thread_id) noexcept
    {
        for (auto & worker_thread_id : worker_thread_ids)
        {
            UInt64 current = worker_thread_id.load(std::memory_order_acquire);
            if (current == thread_id)
                return;

            if (current == 0
                && worker_thread_id.compare_exchange_strong(current, thread_id, std::memory_order_acq_rel, std::memory_order_acquire))
                return;

            if (current == thread_id)
                return;
        }
    }

    size_t distinctWorkers() const
    {
        size_t result = 0;
        for (const auto & thread_id : worker_thread_ids)
            result += thread_id.load(std::memory_order_relaxed) != 0;
        return result;
    }

    size_t input_states;
    Stopwatch wall_watch;
    std::atomic<UInt64> cpu_time_nanoseconds = 0;
    std::atomic<size_t> processed_items = 0;
    VectorWithMemoryTracking<std::atomic<UInt64>> worker_thread_ids;
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
        UInt64 cpu_nanoseconds = 0;
        if (cpu_clock_available && clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end) == 0)
        {
            const UInt64 start_nanoseconds = static_cast<UInt64>(cpu_start.tv_sec) * 1'000'000'000 + cpu_start.tv_nsec;
            const UInt64 end_nanoseconds = static_cast<UInt64>(cpu_end.tv_sec) * 1'000'000'000 + cpu_end.tv_nsec;
            if (end_nanoseconds >= start_nanoseconds)
                cpu_nanoseconds = end_nanoseconds - start_nanoseconds;
        }
        stats.recordTask(cpu_nanoseconds, processed_items);
    }

    void recordWorkItem() noexcept { ++processed_items; }

    UniqExactMergeWaveTaskTimer(const UniqExactMergeWaveTaskTimer &) = delete;
    UniqExactMergeWaveTaskTimer & operator=(const UniqExactMergeWaveTaskTimer &) = delete;

private:
    UniqExactMergeWaveStats & stats;
    timespec cpu_start{};
    size_t processed_items = 0;
    bool cpu_clock_available = false;
};

}
