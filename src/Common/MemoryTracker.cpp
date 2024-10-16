#include "MemoryTracker.h"

#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/OvercommitTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <Common/TraceSender.h>
#include <Common/VariableContext.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include "config.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>

#endif

#include <atomic>
#include <random>
#include <cstdlib>
#include <string>


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

namespace
{

inline std::string_view toDescription(OvercommitResult result)
{
    switch (result)
    {
    case OvercommitResult::NONE:
        return "";
    case OvercommitResult::DISABLED:
        return "Memory overcommit isn't used. Waiting time or overcommit denominator are set to zero.";
    case OvercommitResult::MEMORY_FREED:
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "OvercommitResult::MEMORY_FREED shouldn't be asked for description");
    case OvercommitResult::SELECTED:
        return "Query was selected to stop by OvercommitTracker.";
    case OvercommitResult::TIMEOUTED:
        return "Waiting timeout for memory to be freed is reached.";
    case OvercommitResult::NOT_ENOUGH_FREED:
        return "Memory overcommit has freed not enough memory.";
    }
}

bool shouldTrackAllocation(Float64 probability, void * ptr)
{
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
    return intHash64(uintptr_t(ptr)) < std::numeric_limits<uint64_t>::max() * probability;
#pragma clang diagnostic pop
}

}

void AllocationTrace::onAllocImpl(void * ptr, size_t size) const
{
    if (sample_probability < 1 && !shouldTrackAllocation(sample_probability, ptr))
        return;

    MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
    DB::TraceSender::send(DB::TraceType::MemorySample, StackTrace(), {.size = Int64(size), .ptr = ptr});
}

void AllocationTrace::onFreeImpl(void * ptr, size_t size) const
{
    if (sample_probability < 1 && !shouldTrackAllocation(sample_probability, ptr))
        return;

    MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
    DB::TraceSender::send(DB::TraceType::MemorySample, StackTrace(), {.size = -Int64(size), .ptr = ptr});
}

namespace ProfileEvents
{
    extern const Event QueryMemoryLimitExceeded;
}

using namespace std::chrono_literals;

static constexpr size_t log_peak_memory_usage_every = 1ULL << 30;

MemoryTracker total_memory_tracker(nullptr, VariableContext::Global);
MemoryTracker background_memory_tracker(&total_memory_tracker, VariableContext::User, false);

MemoryTracker::MemoryTracker(VariableContext level_) : parent(&total_memory_tracker), level(level_) {}
MemoryTracker::MemoryTracker(MemoryTracker * parent_, VariableContext level_) : parent(parent_), level(level_) {}

MemoryTracker::MemoryTracker(MemoryTracker * parent_, VariableContext level_, bool log_peak_memory_usage_in_destructor_)
    : parent(parent_), log_peak_memory_usage_in_destructor(log_peak_memory_usage_in_destructor_), level(level_)
{
}

MemoryTracker::~MemoryTracker()
{
    if ((level == VariableContext::Process || level == VariableContext::User) && peak && log_peak_memory_usage_in_destructor)
    {
        try
        {
            logPeakMemoryUsage();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Exception in Logger, intentionally swallow.
        }
    }
}

void MemoryTracker::logPeakMemoryUsage()
{
    log_peak_memory_usage_in_destructor = false;
    const auto * description = description_ptr.load(std::memory_order_relaxed);
    auto peak_bytes = peak.load(std::memory_order::relaxed);
    if (peak_bytes < 128 * 1024)
        return;
    LOG_DEBUG(getLogger("MemoryTracker"),
        "Peak memory usage{}: {}.", (description ? " " + std::string(description) : ""), ReadableSize(peak_bytes));
}

void MemoryTracker::logMemoryUsage(Int64 current) const
{
    const auto * description = description_ptr.load(std::memory_order_relaxed);
    LOG_DEBUG(getLogger("MemoryTracker"),
        "Current memory usage{}: {}.", (description ? " " + std::string(description) : ""), ReadableSize(current));
}

void MemoryTracker::injectFault() const
{
    if (!memoryTrackerCanThrow(level, true))
    {
        LOG_WARNING(getLogger("MemoryTracker"),
                    "Cannot inject fault at specific point. Uncaught exceptions: {}, stack trace:\n{}",
                    std::uncaught_exceptions(), StackTrace().toString());
        return;
    }

    /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
    MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);

    ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
    const auto * description = description_ptr.load(std::memory_order_relaxed);
    throw DB::Exception(
        DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        "Memory tracker{}{}: fault injected (at specific point)",
        description ? " " : "",
        description ? description : "");
}

void MemoryTracker::debugLogBigAllocationWithoutCheck(Int64 size [[maybe_unused]])
{
    /// Big allocations through allocNoThrow (without checking memory limits) may easily lead to OOM (and it's hard to debug).
    /// Let's find them.
#ifdef DEBUG_OR_SANITIZER_BUILD
    if (size < 0)
        return;

    constexpr Int64 threshold = 16 * 1024 * 1024;   /// The choice is arbitrary (maybe we should decrease it)
    if (size < threshold)
        return;

    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    LOG_TEST(
        getLogger("MemoryTracker"),
        "Too big allocation ({} bytes) without checking memory limits, "
        "it may lead to OOM. Stack trace: {}",
        size,
        StackTrace().toString());
#else
    /// Avoid trash logging in release builds
#endif
}

AllocationTrace MemoryTracker::allocImpl(Int64 size, bool throw_if_memory_exceeded, MemoryTracker * query_tracker, double _sample_probability)
{
    if (size < 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Negative size ({}) is passed to MemoryTracker. It is a bug.", size);

    if (_sample_probability < 0)
        _sample_probability = sample_probability;

    if (!isSizeOkForSampling(size))
        _sample_probability = 0;

    if (MemoryTrackerBlockerInThread::isBlocked(level))
    {
        if (level == VariableContext::Global)
        {
            /// For global memory tracker always update memory usage.
            amount.fetch_add(size, std::memory_order_relaxed);
            rss.fetch_add(size, std::memory_order_relaxed);

            auto metric_loaded = metric.load(std::memory_order_relaxed);
            if (metric_loaded != CurrentMetrics::end())
                CurrentMetrics::add(metric_loaded, size);
        }

        /// Since the MemoryTrackerBlockerInThread should respect the level, we should go to the next parent.
        if (auto * loaded_next = parent.load(std::memory_order_relaxed))
        {
            MemoryTracker * tracker = level == VariableContext::Process ? this : query_tracker;
            return loaded_next->allocImpl(size, throw_if_memory_exceeded, tracker, _sample_probability);
        }

        return AllocationTrace(_sample_probability);
    }

    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      *  we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    Int64 will_be = size ? size + amount.fetch_add(size, std::memory_order_relaxed) : amount.load(std::memory_order_relaxed);
    Int64 will_be_rss = size ? size + rss.fetch_add(size, std::memory_order_relaxed) : rss.load(std::memory_order_relaxed);

    auto metric_loaded = metric.load(std::memory_order_relaxed);
    if (metric_loaded != CurrentMetrics::end() && size)
        CurrentMetrics::add(metric_loaded, size);

    Int64 current_hard_limit = hard_limit.load(std::memory_order_relaxed);
    Int64 current_profiler_limit = profiler_limit.load(std::memory_order_relaxed);

    bool memory_limit_exceeded_ignored = false;

    bool allocation_traced = false;
    if (unlikely(current_profiler_limit && will_be > current_profiler_limit))
    {
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        DB::TraceSender::send(DB::TraceType::Memory, StackTrace(), {.size = size});
        setOrRaiseProfilerLimit((will_be + profiler_step - 1) / profiler_step * profiler_step);
        allocation_traced = true;
    }

    std::bernoulli_distribution fault(fault_probability);
    if (unlikely(fault_probability > 0.0 && fault(thread_local_rng)))
    {
        if (memoryTrackerCanThrow(level, true) && throw_if_memory_exceeded)
        {
            /// Revert
            amount.fetch_sub(size, std::memory_order_relaxed);
            rss.fetch_sub(size, std::memory_order_relaxed);

            /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
            MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);

            ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
            const auto * description = description_ptr.load(std::memory_order_relaxed);
            throw DB::Exception(
                DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                "Memory tracker{}{}: fault injected. Would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
                description ? " " : "",
                description ? description : "",
                formatReadableSizeWithBinarySuffix(will_be),
                size,
                formatReadableSizeWithBinarySuffix(current_hard_limit));
        }

        memory_limit_exceeded_ignored = true;
        debugLogBigAllocationWithoutCheck(size);
    }

    if (unlikely(
            current_hard_limit && (will_be > current_hard_limit || (level == VariableContext::Global && will_be_rss > current_hard_limit))))
    {
        if (memoryTrackerCanThrow(level, false) && throw_if_memory_exceeded)
        {
            OvercommitResult overcommit_result = OvercommitResult::NONE;
            if (auto * overcommit_tracker_ptr = overcommit_tracker.load(std::memory_order_relaxed); overcommit_tracker_ptr != nullptr && query_tracker != nullptr)
                overcommit_result = overcommit_tracker_ptr->needToStopQuery(query_tracker, size);

            if (overcommit_result != OvercommitResult::MEMORY_FREED)
            {
                /// Revert
                amount.fetch_sub(size, std::memory_order_relaxed);
                rss.fetch_sub(size, std::memory_order_relaxed);

                /// Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
                MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
                ProfileEvents::increment(ProfileEvents::QueryMemoryLimitExceeded);
                const auto * description = description_ptr.load(std::memory_order_relaxed);
                throw DB::Exception(
                                    DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                                    "Memory limit{}{} exceeded: "
                                    "would use {} (attempt to allocate chunk of {} bytes), current RSS {}, maximum: {}."
                                    "{}{}",
                                    description ? " " : "",
                                    description ? description : "",
                                    formatReadableSizeWithBinarySuffix(will_be),
                                    size,
                                    formatReadableSizeWithBinarySuffix(rss.load(std::memory_order_relaxed)),
                                    formatReadableSizeWithBinarySuffix(current_hard_limit),
                                    overcommit_result == OvercommitResult::NONE ? "" : " OvercommitTracker decision: ",
                                    toDescription(overcommit_result));
            }

            // If OvercommitTracker::needToStopQuery returned false, it guarantees that enough memory is freed.
            // This memory is already counted in variable `amount` in the moment of `will_be` initialization.
            // Now we just need to update value stored in `will_be`, because it should have changed.
            will_be = amount.load(std::memory_order_relaxed);
        }
        else
        {
            memory_limit_exceeded_ignored = true;
            debugLogBigAllocationWithoutCheck(size);
        }
    }

    bool peak_updated = false;
    /// In case of MEMORY_LIMIT_EXCEEDED was ignored, will_be may include
    /// memory of other allocations, that may fail but not reverted yet, and so
    /// updating peak will be inaccurate.
    if (!memory_limit_exceeded_ignored)
    {
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
            debugLogBigAllocationWithoutCheck(size);
        }
    }

    if (peak_updated && allocation_traced)
    {
        MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
        DB::TraceSender::send(DB::TraceType::MemoryPeak, StackTrace(), {.size = will_be});
    }

    if (auto * loaded_next = parent.load(std::memory_order_relaxed))
    {
        MemoryTracker * tracker = level == VariableContext::Process ? this : query_tracker;
        return loaded_next->allocImpl(size, throw_if_memory_exceeded, tracker, _sample_probability);
    }

    return AllocationTrace(_sample_probability);
}

void MemoryTracker::adjustWithUntrackedMemory(Int64 untracked_memory)
{
    if (untracked_memory > 0)
        std::ignore = allocImpl(untracked_memory, /*throw_if_memory_exceeded*/ false);
    else
        std::ignore = free(-untracked_memory);
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

AllocationTrace MemoryTracker::free(Int64 size, double _sample_probability)
{
    if (_sample_probability < 0)
        _sample_probability = sample_probability;

    if (!isSizeOkForSampling(size))
        _sample_probability = 0;

    if (MemoryTrackerBlockerInThread::isBlocked(level))
    {
        if (level == VariableContext::Global)
        {
            /// For global memory tracker always update memory usage.
            amount.fetch_sub(size, std::memory_order_relaxed);
            rss.fetch_sub(size, std::memory_order_relaxed);
            auto metric_loaded = metric.load(std::memory_order_relaxed);
            if (metric_loaded != CurrentMetrics::end())
                CurrentMetrics::sub(metric_loaded, size);
        }

        /// Since the MemoryTrackerBlockerInThread should respect the level, we should go to the next parent.
        if (auto * loaded_next = parent.load(std::memory_order_relaxed))
            return loaded_next->free(size, _sample_probability);

        return AllocationTrace(_sample_probability);
    }

    Int64 accounted_size = size;
    if (level == VariableContext::Global)
    {
        amount.fetch_sub(accounted_size, std::memory_order_relaxed);
        rss.fetch_sub(accounted_size, std::memory_order_relaxed);
    }
    else if (level == VariableContext::Thread)
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
    if (auto * overcommit_tracker_ptr = overcommit_tracker.load(std::memory_order_relaxed))
        overcommit_tracker_ptr->tryContinueQueryExecutionAfterFree(accounted_size);

    /// free should never throw, we can update metric early.
    auto metric_loaded = metric.load(std::memory_order_relaxed);
    if (metric_loaded != CurrentMetrics::end())
        CurrentMetrics::sub(metric_loaded, accounted_size);

    if (auto * loaded_next = parent.load(std::memory_order_relaxed))
        return loaded_next->free(size, _sample_probability);

    return AllocationTrace(_sample_probability);
}


OvercommitRatio MemoryTracker::getOvercommitRatio()
{
    return { amount.load(std::memory_order_relaxed), soft_limit.load(std::memory_order_relaxed) };
}


OvercommitRatio MemoryTracker::getOvercommitRatio(Int64 limit)
{
    return { amount.load(std::memory_order_relaxed), limit };
}


void MemoryTracker::setOvercommitWaitingTime(UInt64 wait_time)
{
    max_wait_time.store(wait_time * 1us, std::memory_order_relaxed);
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


void MemoryTracker::updateRSS(Int64 rss_)
{
    total_memory_tracker.rss.store(rss_, std::memory_order_relaxed);
}

void MemoryTracker::updateAllocated(Int64 allocated_)
{
    Int64 new_amount = allocated_;
    LOG_INFO(
        getLogger("MemoryTracker"),
        "Correcting the value of global memory tracker from {} to {}",
        ReadableSize(total_memory_tracker.amount.load(std::memory_order_relaxed)),
        ReadableSize(allocated_));
    total_memory_tracker.amount.store(new_amount, std::memory_order_relaxed);

    auto metric_loaded = total_memory_tracker.metric.load(std::memory_order_relaxed);
    if (metric_loaded != CurrentMetrics::end())
        CurrentMetrics::set(metric_loaded, new_amount);

    bool log_memory_usage = true;
    total_memory_tracker.updatePeak(new_amount, log_memory_usage);
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

double MemoryTracker::getSampleProbability(UInt64 size)
{
    if (sample_probability >= 0)
    {
        if (!isSizeOkForSampling(size))
            return 0;
        return sample_probability;
    }

    if (auto * loaded_next = parent.load(std::memory_order_relaxed))
        return loaded_next->getSampleProbability(size);

    return 0;
}

bool MemoryTracker::isSizeOkForSampling(UInt64 size) const
{
    /// We can avoid comparison min_allocation_size_bytes with zero, because we cannot have 0 bytes allocation/deallocation
    return ((max_allocation_size_bytes == 0 || size <= max_allocation_size_bytes) && size >= min_allocation_size_bytes);
}

void MemoryTracker::setParent(MemoryTracker * elem)
{
    /// Untracked memory shouldn't be accounted to a query or a user if it was allocated before the thread was attached
    /// to a query thread group or a user group, because this memory will be (🤞) freed outside of these scopes.
    if (level == VariableContext::Thread && DB::current_thread)
        DB::current_thread->flushUntrackedMemory();

    parent.store(elem, std::memory_order_relaxed);
}

bool canEnqueueBackgroundTask()
{
    auto limit = background_memory_tracker.getSoftLimit();
    auto amount = background_memory_tracker.get();
    return limit == 0 || amount < limit;
}
