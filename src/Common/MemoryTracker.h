#pragma once

#include <atomic>
#include <common/types.h>
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
#include <common/scope_guard.h>
extern thread_local bool _memory_tracker_always_throw_logical_error_on_allocation;
#define ALLOCATIONS_IN_SCOPE_IMPL_CONCAT(n, val) \
        bool _allocations_flag_prev_val##n = _memory_tracker_always_throw_logical_error_on_allocation; \
        _memory_tracker_always_throw_logical_error_on_allocation = val; \
        SCOPE_EXIT({ _memory_tracker_always_throw_logical_error_on_allocation = _allocations_flag_prev_val##n; })
#define ALLOCATIONS_IN_SCOPE_IMPL(n, val) ALLOCATIONS_IN_SCOPE_IMPL_CONCAT(n, val)
#define DENY_ALLOCATIONS_IN_SCOPE ALLOCATIONS_IN_SCOPE_IMPL(__LINE__, true)
#define ALLOW_ALLOCATIONS_IN_SCOPE ALLOCATIONS_IN_SCOPE_IMPL(__LINE__, false)
#else
#define DENY_ALLOCATIONS_IN_SCOPE static_assert(true)
#define ALLOW_ALLOCATIONS_IN_SCOPE static_assert(true)
#endif

/** Tracks memory consumption.
  * It throws an exception if amount of consumed memory become greater than certain limit.
  * The same memory tracker could be simultaneously used in different threads.
  */
class MemoryTracker
{
private:
    std::atomic<Int64> amount {0};
    std::atomic<Int64> peak {0};
    std::atomic<Int64> hard_limit {0};
    std::atomic<Int64> profiler_limit {0};

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

    void updatePeak(Int64 will_be, bool log_memory_usage);
    void logMemoryUsage(Int64 current) const;

public:
    MemoryTracker(VariableContext level_ = VariableContext::Thread);
    MemoryTracker(MemoryTracker * parent_, VariableContext level_ = VariableContext::Thread);

    ~MemoryTracker();

    VariableContext level;

    /** Call the following functions before calling of corresponding operations with memory allocators.
      */
    void alloc(Int64 size);

    void allocNoThrow(Int64 size);

    void allocImpl(Int64 size, bool throw_if_memory_exceeded);

    void realloc(Int64 old_size, Int64 new_size)
    {
        Int64 addition = new_size - old_size;
        if (addition > 0)
            alloc(addition);
        else
            free(-addition);
    }

    /** This function should be called after memory deallocation.
      */
    void free(Int64 size);

    Int64 get() const
    {
        return amount.load(std::memory_order_relaxed);
    }

    Int64 getPeak() const
    {
        return peak.load(std::memory_order_relaxed);
    }

    void setHardLimit(Int64 value);

    /** Set limit if it was not set.
      * Otherwise, set limit to new value, if new value is greater than previous limit.
      */
    void setOrRaiseHardLimit(Int64 value);
    void setOrRaiseProfilerLimit(Int64 value);

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

    void setDescription(const char * description)
    {
        description_ptr.store(description, std::memory_order_relaxed);
    }

    /// Reset the accumulated data
    void resetCounters();

    /// Reset the accumulated data and the parent.
    void reset();

    /// Reset current counter to a new value.
    void set(Int64 to);

    /// Prints info about peak memory consumption into log.
    void logPeakMemoryUsage() const;

    /// To be able to temporarily stop memory tracking from current thread.
    struct BlockerInThread
    {
    private:
        BlockerInThread(const BlockerInThread &) = delete;
        BlockerInThread & operator=(const BlockerInThread &) = delete;

        static thread_local uint64_t counter;
        static thread_local VariableContext level;

        VariableContext previous_level;
    public:
        /// level_ - block in level and above
        BlockerInThread(VariableContext level_ = VariableContext::User);
        ~BlockerInThread();

        static bool isBlocked(VariableContext current_level)
        {
            return counter > 0 && current_level >= level;
        }
    };

    /// To be able to avoid MEMORY_LIMIT_EXCEEDED Exception in destructors:
    /// - either configured memory limit reached
    /// - or fault injected
    ///
    /// So this will simply ignore the configured memory limit (and avoid fault injection).
    ///
    /// NOTE: exception will be silently ignored, no message in log
    /// (since logging from MemoryTracker::alloc() is tricky)
    ///
    /// NOTE: MEMORY_LIMIT_EXCEEDED Exception implicitly blocked if
    /// stack unwinding is currently in progress in this thread (to avoid
    /// std::terminate()), so you don't need to use it in this case explicitly.
    struct LockExceptionInThread
    {
    private:
        LockExceptionInThread(const LockExceptionInThread &) = delete;
        LockExceptionInThread & operator=(const LockExceptionInThread &) = delete;

        static thread_local uint64_t counter;
        static thread_local VariableContext level;
        static thread_local bool block_fault_injections;

        VariableContext previous_level;
        bool previous_block_fault_injections;
    public:
        /// level_ - block in level and above
        /// block_fault_injections_ - block in fault injection too
        LockExceptionInThread(VariableContext level_ = VariableContext::User, bool block_fault_injections_ = true);
        ~LockExceptionInThread();

        static bool isBlocked(VariableContext current_level, bool fault_injection)
        {
            return counter > 0 && current_level >= level && (!fault_injection || block_fault_injections);
        }
    };
};

extern MemoryTracker total_memory_tracker;
