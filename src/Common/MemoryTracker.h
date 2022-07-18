#pragma once

#include <atomic>
#include <chrono>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/VariableContext.h>

#if !defined(NDEBUG)
#define MEMORY_TRACKER_DEBUG_CHECKS
#endif

/// DENY_ALLOCATIONS_IN_SCOPE macro makes MemoryTracker throw LOGICAL_ERROR on any allocation attempt
/// until the end of the scope. It's useful to ensure that no allocations happen in signal handlers and
/// outside of try/catch block of thread functions. ALLOW_ALLOCATIONS_IN_SCOPE cancels effect of
/// DENY_ALLOCATIONS_IN_SCOPE in the inner scope. In Release builds these macros do nothing.
#ifdef MEMORY_TRACKER_DEBUG_CHECKS
#include <base/scope_guard.h>
extern thread_local bool memory_tracker_always_throw_logical_error_on_allocation;

/// NOLINTNEXTLINE
#define ALLOCATIONS_IN_SCOPE_IMPL_CONCAT(n, val) \
        bool _allocations_flag_prev_val##n = memory_tracker_always_throw_logical_error_on_allocation; \
        memory_tracker_always_throw_logical_error_on_allocation = val; \
        SCOPE_EXIT({ memory_tracker_always_throw_logical_error_on_allocation = _allocations_flag_prev_val##n; })

/// NOLINTNEXTLINE
#define ALLOCATIONS_IN_SCOPE_IMPL(n, val) ALLOCATIONS_IN_SCOPE_IMPL_CONCAT(n, val)

/// NOLINTNEXTLINE
#define DENY_ALLOCATIONS_IN_SCOPE ALLOCATIONS_IN_SCOPE_IMPL(__LINE__, true)

/// NOLINTNEXTLINE
#define ALLOW_ALLOCATIONS_IN_SCOPE ALLOCATIONS_IN_SCOPE_IMPL(__LINE__, false)
#else
#define DENY_ALLOCATIONS_IN_SCOPE static_assert(true)
#define ALLOW_ALLOCATIONS_IN_SCOPE static_assert(true)
#endif

struct OvercommitRatio;
struct OvercommitTracker;

/** Tracks memory consumption.
  * It throws an exception if amount of consumed memory become greater than certain limit.
  * The same memory tracker could be simultaneously used in different threads.
  *
  * @see LockMemoryExceptionInThread
  * @see MemoryTrackerBlockerInThread
  */
class MemoryTracker
{
private:
    std::atomic<Int64> amount {0};
    std::atomic<Int64> peak {0};
    std::atomic<Int64> soft_limit {0};
    std::atomic<Int64> hard_limit {0};
    std::atomic<Int64> profiler_limit {0};

    static std::atomic<Int64> rss;

    Int64 profiler_step = 0;

    /// To test exception safety of calling code, memory tracker throws an exception on each memory allocation with specified probability.
    double fault_probability = 0;

    /// To randomly sample allocations and deallocations in trace_log.
    double sample_probability = 0;

    /// Singly-linked list. All information will be passed to subsequent memory trackers also (it allows to implement trackers hierarchy).
    /// In terms of tree nodes it is the list of parents. Lifetime of these trackers should "include" lifetime of current tracker.
    std::atomic<MemoryTracker *> parent {};

    /// You could specify custom metric to track memory usage.
    std::atomic<CurrentMetrics::Metric> metric = CurrentMetrics::end();

    /// This description will be used as prefix into log messages (if isn't nullptr)
    std::atomic<const char *> description_ptr = nullptr;

    std::atomic<std::chrono::microseconds> max_wait_time;

    std::atomic<OvercommitTracker *> overcommit_tracker = nullptr;

    bool log_peak_memory_usage_in_destructor = true;

    bool updatePeak(Int64 will_be, bool log_memory_usage);
    void logMemoryUsage(Int64 current) const;

    void setOrRaiseProfilerLimit(Int64 value);

    /// allocImpl(...) and free(...) should not be used directly
    friend struct CurrentMemoryTracker;
    void allocImpl(Int64 size, bool throw_if_memory_exceeded, MemoryTracker * query_tracker = nullptr);
    void free(Int64 size);
public:

    static constexpr auto USAGE_EVENT_NAME = "MemoryTrackerUsage";

    explicit MemoryTracker(VariableContext level_ = VariableContext::Thread);
    explicit MemoryTracker(MemoryTracker * parent_, VariableContext level_ = VariableContext::Thread);

    ~MemoryTracker();

    VariableContext level;

    void adjustWithUntrackedMemory(Int64 untracked_memory);

    Int64 get() const
    {
        return amount.load(std::memory_order_relaxed);
    }

    Int64 getPeak() const
    {
        return peak.load(std::memory_order_relaxed);
    }

    void setSoftLimit(Int64 value);
    void setHardLimit(Int64 value);

    Int64 getHardLimit() const
    {
        return hard_limit.load(std::memory_order_relaxed);
    }
    Int64 getSoftLimit() const
    {
        return soft_limit.load(std::memory_order_relaxed);
    }

    /** Set limit if it was not set.
      * Otherwise, set limit to new value, if new value is greater than previous limit.
      */
    void setOrRaiseHardLimit(Int64 value);

    void setFaultProbability(double value)
    {
        fault_probability = value;
    }

    void setSampleProbability(double value)
    {
        sample_probability = value;
    }

    void setProfilerStep(Int64 value)
    {
        profiler_step = value;
        setOrRaiseProfilerLimit(value);
    }

    /// next should be changed only once: from nullptr to some value.
    /// NOTE: It is not true in MergeListElement
    void setParent(MemoryTracker * elem)
    {
        parent.store(elem, std::memory_order_relaxed);
    }

    MemoryTracker * getParent()
    {
        return parent.load(std::memory_order_relaxed);
    }

    /// The memory consumption could be shown in realtime via CurrentMetrics counter
    void setMetric(CurrentMetrics::Metric metric_)
    {
        metric.store(metric_, std::memory_order_relaxed);
    }

    CurrentMetrics::Metric getMetric()
    {
        return metric.load(std::memory_order_relaxed);
    }

    void setDescription(const char * description)
    {
        description_ptr.store(description, std::memory_order_relaxed);
    }

    OvercommitRatio getOvercommitRatio();
    OvercommitRatio getOvercommitRatio(Int64 limit);

    std::chrono::microseconds getOvercommitWaitingTime()
    {
        return max_wait_time.load(std::memory_order_relaxed);
    }

    void setOvercommitWaitingTime(UInt64 wait_time);

    void setOvercommitTracker(OvercommitTracker * tracker) noexcept
    {
        overcommit_tracker.store(tracker, std::memory_order_relaxed);
    }

    void resetOvercommitTracker() noexcept
    {
        overcommit_tracker.store(nullptr, std::memory_order_relaxed);
    }

    /// Reset the accumulated data
    void resetCounters();

    /// Reset the accumulated data.
    void reset();

    /// Update RSS.
    static void setRSS(Int64 to);

    /// Prints info about peak memory consumption into log.
    void logPeakMemoryUsage();
};

extern MemoryTracker total_memory_tracker;
