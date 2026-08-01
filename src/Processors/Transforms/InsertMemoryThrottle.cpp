#include <Processors/Transforms/InsertMemoryThrottle.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event InsertPipelineThrottled;
}


namespace DB
{

MemorySnapshot QueryMemoryProvider::get() const
{
    MemorySnapshot result;

    MemoryTracker * current_tracker = tracker;
    if (!current_tracker)
        current_tracker = CurrentThread::getMemoryTracker();

    if (!current_tracker)
        return result;

    /// walk the tracker hierarchy and pick the tightest (lowest free) hard limit
    static constexpr int MAX_ITERATIONS = 10;
    int iterations = 0;

    Int64 min_free_memory = std::numeric_limits<Int64>::max();
    bool found_any_limit = false;

    while (current_tracker && iterations < MAX_ITERATIONS)
    {
        Int64 limit = current_tracker->getHardLimit();
        if (limit > 0)
        {
            Int64 used = current_tracker->get();
            Int64 free = limit - used;

            if (free < min_free_memory)
            {
                min_free_memory = free;
                result.used_bytes = used;
                result.hard_limit_bytes = limit;
                found_any_limit = true;
            }
        }

        current_tracker = current_tracker->getParent();
        ++iterations;
    }

    if (!found_any_limit)
    {
        result.used_bytes = 0;
        result.hard_limit_bytes = 0;
    }

    return result;
}


void InsertMemoryThrottle::observeChunkBytes(size_t bytes)
{
    if (bytes == 0)
        return;

    const Int64 sample = static_cast<Int64>(bytes);
    Int64 prev = avg_chunk_bytes.load(std::memory_order_relaxed);
    Int64 next;
    do
    {
        if (prev == 0)
        {
            next = sample;
        }
        else
        {
            const double smoothed = EWMA_ALPHA * static_cast<double>(sample)
                                  + (1.0 - EWMA_ALPHA) * static_cast<double>(prev);
            next = std::max<Int64>(1, static_cast<Int64>(smoothed));
        }
    }
    while (!avg_chunk_bytes.compare_exchange_weak(prev, next, std::memory_order_relaxed));
}


size_t InsertMemoryThrottle::calculateAllowedOutputs(size_t max_outputs, size_t min_outputs)
{
    MemorySnapshot mem = mem_provider->get();

    if (mem.hard_limit_bytes <= 0)
        return max_outputs;

    Int64 free_memory = mem.hard_limit_bytes - mem.used_bytes;
    if (free_memory <= 0)
    {
        ProfileEvents::increment(ProfileEvents::InsertPipelineThrottled);
        LOG_DEBUG(::getLogger("InsertMemoryThrottle"),
            "Memory limit reached: used {} >= limit {}, throttling to {} outputs",
            mem.used_bytes, mem.hard_limit_bytes, min_outputs);
        return min_outputs;
    }

    Int64 chunk_bytes = avg_chunk_bytes.load(std::memory_order_relaxed);
    if (chunk_bytes <= 0)
        return max_outputs;

    Int64 inflated = std::max(
        static_cast<Int64>(1),
        static_cast<Int64>(static_cast<double>(chunk_bytes) * safety_factor));
    size_t calculated_outputs = static_cast<size_t>(free_memory / inflated);
    size_t allowed = std::clamp(calculated_outputs, min_outputs, max_outputs);

    if (allowed < max_outputs)
    {
        ProfileEvents::increment(ProfileEvents::InsertPipelineThrottled);
        LOG_DEBUG(::getLogger("InsertMemoryThrottle"),
            "Throttling: free_memory={} bytes, avg_chunk_bytes={} bytes, safety_factor={:.1f}, "
            "allowed_outputs={}/{}",
            free_memory, chunk_bytes, safety_factor, allowed, max_outputs);
    }

    return allowed;
}

}
