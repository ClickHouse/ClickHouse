#include "MemoryTracker.h"

#include <IO/WriteHelpers.h>
#include "Common/TraceCollector.h"
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <common/logger_useful.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <cmath>
#include <random>
#include <cstdlib>

namespace
{

MemoryTracker * getMemoryTracker()
{
    if (auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
        return thread_memory_tracker;

    /// Once the main thread is initialized,
    /// total_memory_tracker is initialized too.
    /// And can be used, since MainThreadStatus is required for profiling.
    if (DB::MainThreadStatus::get())
        return &total_memory_tracker;

    return nullptr;
}

/// MemoryTracker cannot throw MEMORY_LIMIT_EXCEEDED (either configured memory
/// limit reached or fault injected), in the following cases:
///
/// - when it is explicitly blocked with LockExceptionInThread
///
/// - to avoid std::terminate(), when stack unwinding is current in progress in
///   this thread.
///
///   NOTE: that since C++11 destructor marked with noexcept by default, and
///   this means that any throw from destructor (that is not marked with
///   noexcept(false)) will cause std::terminate()
bool inline memoryTrackerCanThrow()
{
    return !MemoryTracker::LockExceptionInThread::isBlocked() && !std::uncaught_exceptions();
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

thread_local uint64_t MemoryTracker::BlockerInThread::counter = 0;
thread_local uint64_t MemoryTracker::LockExceptionInThread::counter = 0;

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


void MemoryTracker::alloc(Int64 size)
{
    if (size < 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Negative size ({}) is passed to MemoryTracker. It is a bug.", size);

    if (BlockerInThread::isBlocked())
        return;

    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      *  we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);

    if (metric != CurrentMetrics::end())
        CurrentMetrics::add(metric, size);

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

    std::bernoulli_distribution fault(fault_probability);
    if (unlikely(fault_probability && fault(thread_local_rng)) && memoryTrackerCanThrow())
    {
        /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
        BlockerInThread untrack_lock;

        ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
        const auto * description = description_ptr.load(std::memory_order_relaxed);
        amount.fetch_sub(size, std::memory_order_relaxed);
        throw DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                            "Memory tracker{}{}: fault injected. Would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
                            description ? " " : "", description ? description : "",
                            formatReadableSizeWithBinarySuffix(will_be),
                            size, formatReadableSizeWithBinarySuffix(current_hard_limit));
    }

    if (unlikely(current_profiler_limit && will_be > current_profiler_limit))
    {
        BlockerInThread untrack_lock;
        DB::TraceCollector::collect(DB::TraceType::Memory, StackTrace(), size);
        setOrRaiseProfilerLimit((will_be + profiler_step - 1) / profiler_step * profiler_step);
    }

    std::bernoulli_distribution sample(sample_probability);
    if (unlikely(sample_probability && sample(thread_local_rng)))
    {
        BlockerInThread untrack_lock;
        DB::TraceCollector::collect(DB::TraceType::MemorySample, StackTrace(), size);
    }

    if (unlikely(current_hard_limit && will_be > current_hard_limit) && memoryTrackerCanThrow())
    {
        /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
        BlockerInThread untrack_lock;

        ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
        const auto * description = description_ptr.load(std::memory_order_relaxed);
        amount.fetch_sub(size, std::memory_order_relaxed);
        throw DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                            "Memory limit{}{} exceeded: would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
                            description ? " " : "", description ? description : "",
                            formatReadableSizeWithBinarySuffix(will_be),
                            size, formatReadableSizeWithBinarySuffix(current_hard_limit));
    }

    updatePeak(will_be);

    if (auto * loaded_next = parent.load(std::memory_order_relaxed))
        loaded_next->alloc(size);
}


void MemoryTracker::updatePeak(Int64 will_be)
{
    auto peak_old = peak.load(std::memory_order_relaxed);
    if (will_be > peak_old)        /// Races doesn't matter. Could rewrite with CAS, but not worth.
    {
        peak.store(will_be, std::memory_order_relaxed);

        if ((level == VariableContext::Process || level == VariableContext::Global)
            && will_be / log_peak_memory_usage_every > peak_old / log_peak_memory_usage_every)
            logMemoryUsage(will_be);
    }
}


void MemoryTracker::free(Int64 size)
{
    if (BlockerInThread::isBlocked())
        return;

    std::bernoulli_distribution sample(sample_probability);
    if (unlikely(sample_probability && sample(thread_local_rng)))
    {
        BlockerInThread untrack_lock;
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

    if (metric != CurrentMetrics::end())
        CurrentMetrics::sub(metric, accounted_size);
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


void MemoryTracker::set(Int64 to)
{
    amount.store(to, std::memory_order_relaxed);
    updatePeak(to);
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
    Int64 old_value = profiler_limit.load(std::memory_order_relaxed);
    while (old_value < value && !profiler_limit.compare_exchange_weak(old_value, value))
        ;
}


namespace CurrentMemoryTracker
{
    using DB::current_thread;

    void alloc(Int64 size)
    {
        if (auto * memory_tracker = getMemoryTracker())
        {
            if (current_thread)
            {
                current_thread->untracked_memory += size;
                if (current_thread->untracked_memory > current_thread->untracked_memory_limit)
                {
                    /// Zero untracked before track. If tracker throws out-of-limit we would be able to alloc up to untracked_memory_limit bytes
                    /// more. It could be useful to enlarge Exception message in rethrow logic.
                    Int64 tmp = current_thread->untracked_memory;
                    current_thread->untracked_memory = 0;
                    memory_tracker->alloc(tmp);
                }
            }
            /// total_memory_tracker only, ignore untracked_memory
            else
            {
                memory_tracker->alloc(size);
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
        if (auto * memory_tracker = getMemoryTracker())
        {
            if (current_thread)
            {
                current_thread->untracked_memory -= size;
                if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
                {
                    memory_tracker->free(-current_thread->untracked_memory);
                    current_thread->untracked_memory = 0;
                }
            }
            /// total_memory_tracker only, ignore untracked_memory
            else
            {
                memory_tracker->free(size);
            }
        }
    }
}
