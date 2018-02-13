#pragma once

#include <atomic>
#include <common/Types.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
}


/** Tracks memory consumption.
  * It throws an exception if amount of consumed memory become greater than certain limit.
  * The same memory tracker could be simultaneously used in different threads.
  */
class MemoryTracker
{
    std::atomic<Int64> amount {0};
    std::atomic<Int64> peak {0};
    std::atomic<Int64> limit {0};

    /// To test exception safety of calling code, memory tracker throws an exception on each memory allocation with specified probability.
    double fault_probability = 0;

    /// Singly-linked list. All information will be passed to subsequent memory trackers also (it allows to implement trackers hierarchy).
    /// In terms of tree nodes it is the list of parents. Lifetime of these trackers should "include" lifetime of current tracker.
    std::atomic<MemoryTracker *> next {};

    /// You could specify custom metric to track memory usage.
    CurrentMetrics::Metric metric = CurrentMetrics::MemoryTracking;

    /// This description will be used as prefix into log messages (if isn't nullptr)
    const char * description = nullptr;

public:
    MemoryTracker() {}
    MemoryTracker(Int64 limit_) : limit(limit_) {}

    ~MemoryTracker();

    /** Call the following functions before calling of corresponding operations with memory allocators.
      */
    void alloc(Int64 size);

    void realloc(Int64 old_size, Int64 new_size)
    {
        alloc(new_size - old_size);
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

    void setLimit(Int64 limit_)
    {
        limit.store(limit_, std::memory_order_relaxed);
    }

    /** Set limit if it was not set.
      * Otherwise, set limit to new value, if new value is greater than previous limit.
      */
    void setOrRaiseLimit(Int64 value);

    void setFaultProbability(double value)
    {
        fault_probability = value;
    }

    /// next should be changed only once: from nullptr to some value.
    void setNext(MemoryTracker * elem)
    {
        next.store(elem, std::memory_order_relaxed);
    }

    /// The memory consumption could be shown in realtime via CurrentMetrics counter
    void setMetric(CurrentMetrics::Metric metric_)
    {
        metric = metric_;
    }

    void setDescription(const char * description_)
    {
        description = description_;
    }

    /// Reset the accumulated data.
    void reset();

    /// Prints info about peak memory consumption into log.
    void logPeakMemoryUsage() const;
};


/** The MemoryTracker object is quite difficult to pass to all places where significant amounts of memory are allocated.
  * Therefore, a thread-local pointer to used MemoryTracker is set, or nullptr if MemoryTracker does not need to be used.
  * This pointer is set when memory consumption is monitored in current thread.
  * So, you just need to pass it to all the threads that handle one request.
  */
#if __APPLE__ && __clang__
extern __thread MemoryTracker * current_memory_tracker;
#else
extern thread_local MemoryTracker * current_memory_tracker;
#endif

/// Convenience methods, that use current_memory_tracker if it is available.
namespace CurrentMemoryTracker
{
    void alloc(Int64 size);
    void realloc(Int64 old_size, Int64 new_size);
    void free(Int64 size);
}


#include <boost/noncopyable.hpp>

struct TemporarilyDisableMemoryTracker : private boost::noncopyable
{
    MemoryTracker * memory_tracker;

    TemporarilyDisableMemoryTracker()
    {
        memory_tracker = current_memory_tracker;
        current_memory_tracker = nullptr;
    }

    ~TemporarilyDisableMemoryTracker()
    {
        current_memory_tracker = memory_tracker;
    }
};
