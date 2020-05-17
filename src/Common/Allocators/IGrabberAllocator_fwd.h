#pragma once

#include "allocatorCommon.h"
#include <functional>

namespace ga
{

/// Default parameter for two of allocator's template parameters, serves as a placeholder.
struct Runtime {};

template <class Value>
static constexpr size_t const defaultValueAlignment = std::max(16lu, alignof(Value));

[[nodiscard, gnu::const]] static constexpr size_t roundUp(size_t x, size_t rounding) noexcept
{
    return (x + (rounding - 1)) / rounding * rounding;
}

/// Not in std for some sake.
template<class T, class... Args>
constexpr T* construct_at(void* p, Args&&... args)
{
    return ::new (p) T(std::forward<Args>(args)...);
}

struct Stats
{
    size_t total_chunks_size {0};
    size_t total_allocated_size {0};
    size_t total_currently_initialized_size {0};
    size_t total_currently_used_size {0};

    size_t chunks_count{0};
    size_t all_regions_count {0};
    size_t free_regions_count {0};
    size_t used_regions_count {0};
    size_t keyed_regions_count {0};

    size_t hits {0};
    size_t concurrent_hits {0};
    size_t misses {0};

    size_t allocations {0};
    size_t allocated_bytes {0};
    size_t evictions {0};
    size_t evicted_bytes {0};
    size_t secondary_evictions {0};
};
}

namespace DB
{
template <
    class TKey,
    class TValue,
    class KeyHash = std::hash<TKey>,
    class SizeFunction = ga::Runtime,
    class InitFunction = ga::Runtime,
    class ASLRFunction = AllocatorsASLR,

    size_t MinChunkSize = MMAP_THRESHOLD,
    size_t ValueAlignment = ga::defaultValueAlignment<TValue>>
class IGrabberAllocator;

struct FakePODAllocForIG;
struct FakeMemoryAllocForIG;
}

