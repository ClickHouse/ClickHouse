#pragma once

#include "config.h"

#include <array>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <utility>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace DB
{

struct JemallocAllocationStats
{
    uint64_t allocations = 0;
    uint64_t allocated_bytes = 0;
};

/// Measures allocation requests and allocated bytes on the calling thread's jemalloc arena.
///
/// Reading exact request counts requires flushing the calling thread's tcache. This deliberately
/// changes allocator state, so use the counter for a separate diagnostic probe outside a benchmark's
/// timed loop. The measured callback must stay on the constructing thread and its current arena.
/// Request counts are arena-wide, so the probe also assumes no concurrent work uses that arena.
class JemallocAllocationCounter
{
public:
    JemallocAllocationCounter()
    {
#if USE_JEMALLOC
        bool stats_enabled = false;
        size_t value_size = sizeof(stats_enabled);
        if (je_mallctl("config.stats", &stats_enabled, &value_size, nullptr, 0) != 0 || !stats_enabled)
            return;

        value_size = sizeof(thread_allocated_bytes);
        if (je_mallctl("thread.allocatedp", &thread_allocated_bytes, &value_size, nullptr, 0) != 0 || !thread_allocated_bytes)
            return;

        value_size = sizeof(thread_cache_enabled);
        if (je_mallctl("thread.tcache.enabled", &thread_cache_enabled, &value_size, nullptr, 0) != 0)
            return;

        unsigned arena = 0;
        value_size = sizeof(arena);
        if (je_mallctl("thread.arena", &arena, &value_size, nullptr, 0) != 0)
            return;

        char small_requests_name[64];
        char large_requests_name[64];
        const int small_length = std::snprintf(small_requests_name, sizeof(small_requests_name), "stats.arenas.%u.small.nrequests", arena);
        const int large_length = std::snprintf(large_requests_name, sizeof(large_requests_name), "stats.arenas.%u.large.nrequests", arena);
        if (small_length <= 0 || static_cast<size_t>(small_length) >= sizeof(small_requests_name) || large_length <= 0
            || static_cast<size_t>(large_length) >= sizeof(large_requests_name))
            return;

        small_requests_mib_length = small_requests_mib.size();
        large_requests_mib_length = large_requests_mib.size();
        if (je_mallctlnametomib(small_requests_name, small_requests_mib.data(), &small_requests_mib_length) != 0
            || je_mallctlnametomib(large_requests_name, large_requests_mib.data(), &large_requests_mib_length) != 0)
            return;

        available = true;
#endif
    }

    JemallocAllocationCounter(const JemallocAllocationCounter &) = delete;
    JemallocAllocationCounter & operator=(const JemallocAllocationCounter &) = delete;

    bool isAvailable() const { return available; }

    bool start()
    {
#if USE_JEMALLOC
        if (!available || started || !flushThreadCacheAndRefreshStats() || !readAllocationRequests(requests_before))
            return false;

        allocated_bytes_before = *thread_allocated_bytes;
        started = true;
        return true;
#else
        return false;
#endif
    }

    std::optional<JemallocAllocationStats> stop()
    {
#if USE_JEMALLOC
        if (!started)
            return std::nullopt;

        const uint64_t allocated_bytes_after = *thread_allocated_bytes;
        uint64_t requests_after = 0;
        started = false;
        if (!flushThreadCacheAndRefreshStats() || !readAllocationRequests(requests_after))
        {
            available = false;
            return std::nullopt;
        }

        return JemallocAllocationStats{
            .allocations = requests_after - requests_before, .allocated_bytes = allocated_bytes_after - allocated_bytes_before};
#else
        return std::nullopt;
#endif
    }

private:
#if USE_JEMALLOC
    bool flushThreadCacheAndRefreshStats() const
    {
        if (thread_cache_enabled && je_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0) != 0)
            return false;

        uint64_t epoch = 1;
        size_t epoch_size = sizeof(epoch);
        return je_mallctl("epoch", &epoch, &epoch_size, &epoch, sizeof(epoch)) == 0;
    }

    bool readAllocationRequests(uint64_t & requests) const
    {
        uint64_t small_requests = 0;
        uint64_t large_requests = 0;
        size_t value_size = sizeof(small_requests);
        if (je_mallctlbymib(small_requests_mib.data(), small_requests_mib_length, &small_requests, &value_size, nullptr, 0) != 0)
            return false;

        value_size = sizeof(large_requests);
        if (je_mallctlbymib(large_requests_mib.data(), large_requests_mib_length, &large_requests, &value_size, nullptr, 0) != 0)
            return false;

        requests = small_requests + large_requests;
        return true;
    }

    static constexpr size_t request_mib_capacity = 6;
    std::array<size_t, request_mib_capacity> small_requests_mib{};
    std::array<size_t, request_mib_capacity> large_requests_mib{};
    size_t small_requests_mib_length = 0;
    size_t large_requests_mib_length = 0;
    uint64_t * thread_allocated_bytes = nullptr;
    uint64_t requests_before = 0;
    bool thread_cache_enabled = false;
    uint64_t allocated_bytes_before = 0;
#endif
    bool available = false;
    bool started = false;
};

template <typename Callback>
std::optional<JemallocAllocationStats> measureJemallocAllocations(Callback && callback)
{
    JemallocAllocationCounter counter;
    if (!counter.start())
        return std::nullopt;

    std::forward<Callback>(callback)();
    return counter.stop();
}

}
