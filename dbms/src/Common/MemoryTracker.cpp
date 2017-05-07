#include <common/likely.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <IO/WriteHelpers.h>
#include <iomanip>

#include <Common/MemoryTracker.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int MEMORY_LIMIT_EXCEEDED;
    }
}


MemoryTracker::~MemoryTracker()
{
    if (peak)
        logPeakMemoryUsage();

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
    if (amount)
        free(amount);
}


void MemoryTracker::logPeakMemoryUsage() const
{
    LOG_DEBUG(&Logger::get("MemoryTracker"),
        "Peak memory usage" << (description ? " " + std::string(description) : "")
        << ": " << formatReadableSizeWithBinarySuffix(peak) << ".");
}


void MemoryTracker::alloc(Int64 size)
{
    Int64 will_be = amount += size;

    if (!next)
        CurrentMetrics::add(metric, size);

    Int64 current_limit = limit.load(std::memory_order_relaxed);

    /// Using non-thread-safe random number generator. Joint distribution in different threads would not be uniform.
    /// In this case, it doesn't matter.
    if (unlikely(fault_probability && drand48() < fault_probability))
    {
        free(size);

        std::stringstream message;
        message << "Memory tracker";
        if (description)
            message << " " << description;
        message << ": fault injected. Would use " << formatReadableSizeWithBinarySuffix(will_be)
            << " (attempt to allocate chunk of " << size << " bytes)"
            << ", maximum: " << formatReadableSizeWithBinarySuffix(current_limit);

        throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    if (unlikely(current_limit && will_be > current_limit))
    {
        free(size);

        std::stringstream message;
        message << "Memory limit";
        if (description)
            message << " " << description;
        message << " exceeded: would use " << formatReadableSizeWithBinarySuffix(will_be)
            << " (attempt to allocate chunk of " << size << " bytes)"
            << ", maximum: " << formatReadableSizeWithBinarySuffix(current_limit);

        throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    if (will_be > peak.load(std::memory_order_relaxed))        /// Races doesn't matter. Could rewrite with CAS, but not worth.
        peak.store(will_be, std::memory_order_relaxed);

    if (next)
        next->alloc(size);
}


void MemoryTracker::free(Int64 size)
{
    /** Sometimes, query could free some data, that was allocated outside of query context.
      * Example: cache eviction.
      * To avoid negative memory usage, we "saturate" amount.
      * Memory usage will be calculated with some error.
      */
    Int64 size_to_subtract = size;
    if (size_to_subtract > amount)
        size_to_subtract = amount;

    amount -= size_to_subtract;
    /// NOTE above code is not atomic. It's easy to fix.

    if (next)
        next->free(size);
    else
        CurrentMetrics::sub(metric, size_to_subtract);
}


void MemoryTracker::reset()
{
    if (!next)
        CurrentMetrics::sub(metric, amount);

    amount.store(0, std::memory_order_relaxed);
    peak.store(0, std::memory_order_relaxed);
    limit.store(0, std::memory_order_relaxed);
}


void MemoryTracker::setOrRaiseLimit(Int64 value)
{
    /// This is just atomic set to maximum.
    Int64 old_value = limit.load(std::memory_order_relaxed);
    while (old_value < value && !limit.compare_exchange_weak(old_value, value))
        ;
}


__thread MemoryTracker * current_memory_tracker = nullptr;

namespace CurrentMemoryTracker
{
    void alloc(Int64 size)
    {
        if (current_memory_tracker)
            current_memory_tracker->alloc(size);
    }

    void realloc(Int64 old_size, Int64 new_size)
    {
        if (current_memory_tracker)
            current_memory_tracker->alloc(new_size - old_size);
    }

    void free(Int64 size)
    {
        if (current_memory_tracker)
            current_memory_tracker->free(size);
    }
}
