#include "MemoryTracker.h"

#include <IO/WriteHelpers.h>
#include <Common/VariableContext.h>
#include <Interpreters/TraceCollector.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>
#include <Common/thread_local_rng.h>
#include <Common/OvercommitTracker.h>
#include <base/logger_useful.h>

#include <atomic>
#include <cmath>
#include <random>
#include <cstdlib>


#ifdef MEMORY_TRACKER_DEBUG_CHECKS
thread_local bool memory_tracker_always_throw_logical_error_on_allocation = false;
#endif

namespace
{

/// MemoryTracker cannot throw MEMORY_LIMIT_EXCEEDED (either configured memory
/// limit reached or fault injected), in the following cases:
///
/// - when it is explicitly blocked with LockExceptionInThread
///
/// - when there are uncaught exceptions objects in the current thread
///   (to avoid std::terminate())
///
///   NOTE: that since C++11 destructor marked with noexcept by default, and
///   this means that any throw from destructor (that is not marked with
///   noexcept(false)) will cause std::terminate()
bool inline memoryTrackerCanThrow(VariableContext level, bool fault_injection)
{
    return !LockMemoryExceptionInThread::isBlocked(level, fault_injection) && !std::uncaught_exceptions();
}

}

namespace DB
{
    namespace ErrorCodes
    {
        extern const int MEMORY_LIMIT_EXCEEDED;
        extern const int LOGICAL_ERROR;
    }
}

namespace ProfileEvents
{
    extern const Event QueryMemoryLimitExceeded;
}

static constexpr size_t log_peak_memory_usage_every = 1ULL << 30;

MemoryTracker total_memory_tracker(nullptr, VariableContext::Global);


MemoryTracker::MemoryTracker(VariableContext level_) : parent(&total_memory_tracker), level(level_) {}
MemoryTracker::MemoryTracker(MemoryTracker * parent_, VariableContext level_) : parent(parent_), level(level_) {}


MemoryTracker::~MemoryTracker()
{
    if ((level == VariableContext::Process || level == VariableContext::User) && peak)
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
}


void MemoryTracker::logPeakMemoryUsage() const
{
    const auto * description = description_ptr.load(std::memory_order_relaxed);
    LOG_DEBUG(&Poco::Logger::get("MemoryTracker"),
        "Peak memory usage{}: {}.", (description ? " " + std::string(description) : ""), ReadableSize(peak));
}

void MemoryTracker::logMemoryUsage(Int64 current) const
{
    const auto * description = description_ptr.load(std::memory_order_relaxed);
    LOG_DEBUG(&Poco::Logger::get("MemoryTracker"),
        "Current memory usage{}: {}.", (description ? " " + std::string(description) : ""), ReadableSize(current));
}


void MemoryTracker::allocImpl(Int64 size, bool throw_if_memory_exceeded, MemoryTracker * query_tracker)
{
    if (size < 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Negative size ({}) is passed to MemoryTracker. It is a bug.", size);

    if (MemoryTrackerBlockerInThread::isBlocked(level))
    {
        /// Since the MemoryTrackerBlockerInThread should respect the level, we should go to the next parent.
        if (auto * loaded_next = parent.load(std::memory_order_relaxed))
            loaded_next->allocImpl(size, throw_if_memory_exceeded,
                level == VariableContext::Process ? this : query_tracker);
        return;
    }

    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      *  we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);

    auto metric_loaded = metric.load(std::memory_order_relaxed);
    if (metric_loaded != CurrentMetrics::end())
        CurrentMetrics::add(metric_loaded, size);

    Int64 current_hard_limit = hard_limit.load(std::memory_order_relaxed);
    Int64 current_profiler_limit = profiler_limit.load(std::memory_order_relaxed);

    /// Cap the limit to the total_memory_tracker, since it may include some drift
    /// for user-level memory tracker.
    ///
    /// And since total_memory_tracker is reset to the process resident
    /// memory peridically (in AsynchronousMetrics::update()), any limit can be
    /// capped to it, to avoid possible drift.
    if (unlikely(current_hard_limit
        && will_be > current_hard_limit
        && level == VariableContext::User))
    {
        Int64 total_amount = total_memory_tracker.get();
        if (amount > total_amount)
        {
            set(total_amount);
            will_be = size + total_amount;
        }
    }

#ifdef MEMORY_TRACKER_DEBUG_CHECKS
    if (unlikely(memory_tracker_always_throw_logical_error_on_allocation))
    {
        memory_tracker_always_throw_logical_error_on_allocation = false;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Memory tracker: allocations not allowed.");
    }
#endif

    std::bernoulli_distribution fault(fault_probability);
    if (unlikely(fault_probability && fault(thread_local_rng)) && memoryTrackerCanThrow(level, true) && throw_if_memory_exceeded)
    {
        /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);

        ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
        const auto * description = description_ptr.load(std::memory_order_relaxed);
        amount.fetch_sub(size, std::memory_order_relaxed);
        throw DB::Exception(
            DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
            "Memory tracker{}{}: fault injected. Would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
            description ? " " : "",
            description ? description : "",
            formatReadableSizeWithBinarySuffix(will_be),
            size,
            formatReadableSizeWithBinarySuffix(current_hard_limit));
    }


    bool allocation_traced = false;
    if (unlikely(current_profiler_limit && will_be > current_profiler_limit))
    {
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        DB::TraceCollector::collect(DB::TraceType::Memory, StackTrace(), size);
        setOrRaiseProfilerLimit((will_be + profiler_step - 1) / profiler_step * profiler_step);
        allocation_traced = true;
    }

    std::bernoulli_distribution sample(sample_probability);
    if (unlikely(sample_probability && sample(thread_local_rng)))
    {
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        DB::TraceCollector::collect(DB::TraceType::MemorySample, StackTrace(), size);
        allocation_traced = true;
    }

    if (unlikely(current_hard_limit && will_be > current_hard_limit) && memoryTrackerCanThrow(level, false) && throw_if_memory_exceeded)
    {
        bool need_to_throw = true;
        bool try_to_free_memory = overcommit_tracker != nullptr && query_tracker != nullptr;
        if (try_to_free_memory)
            need_to_throw = overcommit_tracker->needToStopQuery(query_tracker);

        if (need_to_throw)
        {
            /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
            MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
            ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
            const auto * description = description_ptr.load(std::memory_order_relaxed);
            throw DB::Exception(
                DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                "Memory limit{}{} exceeded: would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
                description ? " " : "",
                description ? description : "",
                formatReadableSizeWithBinarySuffix(will_be),
                size,
                formatReadableSizeWithBinarySuffix(current_hard_limit));
        }
        else
        {
            will_be = amount.load(std::memory_order_relaxed);
        }
    }

    bool peak_updated;
    if (throw_if_memory_exceeded)
    {
        /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        bool log_memory_usage = true;
        peak_updated = updatePeak(will_be, log_memory_usage);
    }
    else
    {
        bool log_memory_usage = false;
        peak_updated = updatePeak(will_be, log_memory_usage);
    }

    if (peak_updated && allocation_traced)
    {
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        DB::TraceCollector::collect(DB::TraceType::MemoryPeak, StackTrace(), will_be);
    }

    if (auto * loaded_next = parent.load(std::memory_order_relaxed))
        loaded_next->allocImpl(size, throw_if_memory_exceeded,
            level == VariableContext::Process ? this : query_tracker);
}

void MemoryTracker::alloc(Int64 size)
{
    bool throw_if_memory_exceeded = true;
    allocImpl(size, throw_if_memory_exceeded);
}

void MemoryTracker::allocNoThrow(Int64 size)
{
    bool throw_if_memory_exceeded = false;
    allocImpl(size, throw_if_memory_exceeded);
}

bool MemoryTracker::updatePeak(Int64 will_be, bool log_memory_usage)
{
    auto peak_old = peak.load(std::memory_order_relaxed);
    if (will_be > peak_old)        /// Races doesn't matter. Could rewrite with CAS, but not worth.
    {
        peak.store(will_be, std::memory_order_relaxed);

        if (log_memory_usage && (level == VariableContext::Process || level == VariableContext::Global)
            && will_be / log_peak_memory_usage_every > peak_old / log_peak_memory_usage_every)
            logMemoryUsage(will_be);

        return true;
    }
    return false;
}


void MemoryTracker::free(Int64 size)
{
    if (MemoryTrackerBlockerInThread::isBlocked(level))
    {
        /// Since the MemoryTrackerBlockerInThread should respect the level, we should go to the next parent.
        if (auto * loaded_next = parent.load(std::memory_order_relaxed))
            loaded_next->free(size);
        return;
    }

    std::bernoulli_distribution sample(sample_probability);
    if (unlikely(sample_probability && sample(thread_local_rng)))
    {
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        DB::TraceCollector::collect(DB::TraceType::MemorySample, StackTrace(), -size);
    }

    Int64 accounted_size = size;
    if (level == VariableContext::Thread)
    {
        /// Could become negative if memory allocated in this thread is freed in another one
        amount.fetch_sub(accounted_size, std::memory_order_relaxed);
    }
    else
    {
        Int64 new_amount = amount.fetch_sub(accounted_size, std::memory_order_relaxed) - accounted_size;

        /** Sometimes, query could free some data, that was allocated outside of query context.
          * Example: cache eviction.
          * To avoid negative memory usage, we "saturate" amount.
          * Memory usage will be calculated with some error.
          * NOTE: The code is not atomic. Not worth to fix.
          */
        if (unlikely(new_amount < 0))
        {
            amount.fetch_sub(new_amount);
            accounted_size += new_amount;
        }
    }

    if (auto * loaded_next = parent.load(std::memory_order_relaxed))
        loaded_next->free(size);

    auto metric_loaded = metric.load(std::memory_order_relaxed);
    if (metric_loaded != CurrentMetrics::end())
        CurrentMetrics::sub(metric_loaded, accounted_size);
}


OvercommitRatio MemoryTracker::getOvercommitRatio()
{
    return { amount.load(std::memory_order_relaxed), soft_limit.load(std::memory_order_relaxed) };
}


OvercommitRatio MemoryTracker::getOvercommitRatio(Int64 limit)
{
    return { amount.load(std::memory_order_relaxed), limit };
}


void MemoryTracker::resetCounters()
{
    amount.store(0, std::memory_order_relaxed);
    peak.store(0, std::memory_order_relaxed);
    soft_limit.store(0, std::memory_order_relaxed);
    hard_limit.store(0, std::memory_order_relaxed);
    profiler_limit.store(0, std::memory_order_relaxed);
}


void MemoryTracker::reset()
{
    auto metric_loaded = metric.load(std::memory_order_relaxed);
    if (metric_loaded != CurrentMetrics::end())
        CurrentMetrics::sub(metric_loaded, amount.load(std::memory_order_relaxed));

    resetCounters();
}


void MemoryTracker::set(Int64 to)
{
    amount.store(to, std::memory_order_relaxed);

    bool log_memory_usage = true;
    updatePeak(to, log_memory_usage);
}


void MemoryTracker::setSoftLimit(Int64 value)
{
    soft_limit.store(value, std::memory_order_relaxed);
}


void MemoryTracker::setHardLimit(Int64 value)
{
    hard_limit.store(value, std::memory_order_relaxed);
}


void MemoryTracker::setOrRaiseHardLimit(Int64 value)
{
    /// This is just atomic set to maximum.
    Int64 old_value = hard_limit.load(std::memory_order_relaxed);
    while ((value == 0 || old_value < value) && !hard_limit.compare_exchange_weak(old_value, value))
        ;
}


void MemoryTracker::setOrRaiseProfilerLimit(Int64 value)
{
    Int64 old_value = profiler_limit.load(std::memory_order_relaxed);
    while ((value == 0 || old_value < value) && !profiler_limit.compare_exchange_weak(old_value, value))
        ;
}
