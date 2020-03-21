#include "MemoryTracker.h"

#include <IO/WriteHelpers.h>
#include "Common/TraceCollector.h"
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <ext/singleton.h>

#include <atomic>
#include <cmath>
#include <cstdlib>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int MEMORY_LIMIT_EXCEEDED;
    }
}


static constexpr size_t log_peak_memory_usage_every = 1ULL << 30;
/// Each thread could new/delete memory in range of (-untracked_memory_limit, untracked_memory_limit) without access to common counters.
static constexpr Int64 untracked_memory_limit = 4 * 1024 * 1024;


MemoryTracker::~MemoryTracker()
{
    if (static_cast<int>(level) < static_cast<int>(VariableContext::Process) && peak)
    {
        try
        {
            logPeakMemoryUsage();
        }
        catch (...)
        {
            /// Exception in Logger, intentionally swallow.
        }
    }

    /** This is needed for next memory tracker to be consistent with sum of all referring memory trackers.
      *
      * Sometimes, memory tracker could be destroyed before memory was freed, and on destruction, amount > 0.
      * For example, a query could allocate some data and leave it in cache.
      *
      * If memory will be freed outside of context of this memory tracker,
      *  but in context of one of the 'next' memory trackers,
      *  then memory usage of 'next' memory trackers will be underestimated,
      *  because amount will be decreased twice (first - here, second - when real 'free' happens).
      */
    if (auto value = amount.load(std::memory_order_relaxed))
        free(value);
}


void MemoryTracker::logPeakMemoryUsage() const
{
    LOG_DEBUG(&Logger::get("MemoryTracker"),
        "Peak memory usage" << (description ? " " + std::string(description) : "")
        << ": " << formatReadableSizeWithBinarySuffix(peak) << ".");
}

static void logMemoryUsage(Int64 amount)
{
    LOG_DEBUG(&Logger::get("MemoryTracker"),
        "Current memory usage: " << formatReadableSizeWithBinarySuffix(amount) << ".");
}



void MemoryTracker::alloc(Int64 size)
{
    if (blocker.isCancelled())
        return;

    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      * we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);

    if (metric != CurrentMetrics::end())
        CurrentMetrics::add(metric, size);

    Int64 current_hard_limit = hard_limit.load(std::memory_order_relaxed);
    Int64 current_profiler_limit = profiler_limit.load(std::memory_order_relaxed);

    /// Using non-thread-safe random number generator. Joint distribution in different threads would not be uniform.
    /// In this case, it doesn't matter.
    if (unlikely(fault_probability && drand48() < fault_probability))
    {
        free(size);

        /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
        auto untrack_lock = blocker.cancel();

        std::stringstream message;
        message << "Memory tracker";
        if (description)
            message << " " << description;
        message << ": fault injected. Would use " << formatReadableSizeWithBinarySuffix(will_be)
            << " (attempt to allocate chunk of " << size << " bytes)"
            << ", maximum: " << formatReadableSizeWithBinarySuffix(current_hard_limit);

        throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    if (unlikely(current_profiler_limit && will_be > current_profiler_limit))
    {
        auto no_track = blocker.cancel();
        ext::Singleton<DB::TraceCollector>()->collect(size);
        setOrRaiseProfilerLimit(current_profiler_limit + Int64(std::ceil((will_be - current_profiler_limit) / profiler_step)) * profiler_step);
    }

    if (unlikely(current_hard_limit && will_be > current_hard_limit))
    {
        free(size);

        /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
        auto untrack_lock = blocker.cancel();

        std::stringstream message;
        message << "Memory limit";
        if (description)
            message << " " << description;
        message << " exceeded: would use " << formatReadableSizeWithBinarySuffix(will_be)
            << " (attempt to allocate chunk of " << size << " bytes)"
            << ", maximum: " << formatReadableSizeWithBinarySuffix(current_hard_limit);

        throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    auto peak_old = peak.load(std::memory_order_relaxed);
    if (will_be > peak_old)        /// Races doesn't matter. Could rewrite with CAS, but not worth.
    {
        peak.store(will_be, std::memory_order_relaxed);

        if (level == VariableContext::Process && will_be / log_peak_memory_usage_every > peak_old / log_peak_memory_usage_every)
            logMemoryUsage(will_be);
    }

    if (auto loaded_next = parent.load(std::memory_order_relaxed))
        loaded_next->alloc(size);
}


void MemoryTracker::free(Int64 size)
{
    if (blocker.isCancelled())
        return;

    if (level == VariableContext::Thread)
    {
        /// Could become negative if memory allocated in this thread is freed in another one
        amount.fetch_sub(size, std::memory_order_relaxed);
    }
    else
    {
        Int64 new_amount = amount.fetch_sub(size, std::memory_order_relaxed) - size;

        /** Sometimes, query could free some data, that was allocated outside of query context.
          * Example: cache eviction.
          * To avoid negative memory usage, we "saturate" amount.
          * Memory usage will be calculated with some error.
          * NOTE: The code is not atomic. Not worth to fix.
          */
        if (unlikely(new_amount < 0))
        {
            amount.fetch_sub(new_amount);
            size += new_amount;
        }
    }

    if (auto loaded_next = parent.load(std::memory_order_relaxed))
        loaded_next->free(size);

    if (metric != CurrentMetrics::end())
        CurrentMetrics::sub(metric, size);
}


void MemoryTracker::resetCounters()
{
    amount.store(0, std::memory_order_relaxed);
    peak.store(0, std::memory_order_relaxed);
    hard_limit.store(0, std::memory_order_relaxed);
    profiler_limit.store(0, std::memory_order_relaxed);
}


void MemoryTracker::reset()
{
    if (metric != CurrentMetrics::end())
        CurrentMetrics::sub(metric, amount.load(std::memory_order_relaxed));

    resetCounters();
}


void MemoryTracker::setOrRaiseHardLimit(Int64 value)
{
    /// This is just atomic set to maximum.
    Int64 old_value = hard_limit.load(std::memory_order_relaxed);
    while (old_value < value && !hard_limit.compare_exchange_weak(old_value, value))
        ;
}


void MemoryTracker::setOrRaiseProfilerLimit(Int64 value)
{
    /// This is just atomic set to maximum.
    Int64 old_value = profiler_limit.load(std::memory_order_relaxed);
    while (old_value < value && !profiler_limit.compare_exchange_weak(old_value, value))
        ;
}


namespace CurrentMemoryTracker
{
    void alloc(Int64 size)
    {
        if (auto memory_tracker = DB::CurrentThread::getMemoryTracker())
        {
            Int64 & untracked = DB::CurrentThread::getUntrackedMemory();
            untracked += size;
            if (untracked > untracked_memory_limit)
            {
                /// Zero untracked before track. If tracker throws out-of-limit we would be able to alloc up to untracked_memory_limit bytes
                /// more. It could be useful to enlarge Exception message in rethrow logic.
                Int64 tmp = untracked;
                untracked = 0;
                memory_tracker->alloc(tmp);
            }
        }
    }

    void realloc(Int64 old_size, Int64 new_size)
    {
        Int64 addition = new_size - old_size;
        addition > 0 ? alloc(addition) : free(-addition);
    }

    void free(Int64 size)
    {
        if (auto memory_tracker = DB::CurrentThread::getMemoryTracker())
        {
            Int64 & untracked = DB::CurrentThread::getUntrackedMemory();
            untracked -= size;
            if (untracked < -untracked_memory_limit)
            {
                memory_tracker->free(-untracked);
                untracked = 0;
            }
        }
    }
}

DB::SimpleActionLock getCurrentMemoryTrackerActionLock()
{
    auto memory_tracker = DB::CurrentThread::getMemoryTracker();
    if (!memory_tracker)
        return {};
    return memory_tracker->blocker.cancel();
}
