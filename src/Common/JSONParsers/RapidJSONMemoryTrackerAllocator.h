#pragma once

#include "config.h"

#if USE_RAPIDJSON

#include <cstddef>

namespace DB
{

/// An allocator implementing the rapidjson `Allocator` concept that accounts every allocation
/// against ClickHouse's `MemoryTracker` by delegating to `DB::Allocator`.
///
/// rapidjson's default `CrtAllocator` calls raw `malloc`/`realloc`, bypassing the memory tracker
/// entirely. A pathological input (for example a deeply nested JSON pretty-printed with wide
/// indentation) can then grow rapidjson's internal stacks or output buffer without bound, which
/// either runs the server out of memory or trips the sanitizer's allocation-size cap in tests.
/// Going through `DB::Allocator` turns that into a clean `MEMORY_LIMIT_EXCEEDED` exception bounded
/// by `max_memory_usage`, and reuses its tracking, rollback and large-allocation `mmap` path.
///
/// The only thing this adapter adds on top of `DB::Allocator` is shape: rapidjson needs
/// `Malloc`/`Realloc`/`Free`, and its `Free` is static and is not given the allocation size, while
/// `DB::Allocator::free` requires the size. So the size of every block is stored in a suitably
/// aligned header placed in front of the memory handed out.
class RapidJSONMemoryTrackerAllocator
{
public:
    /// rapidjson must call `Free` to release the blocks we hand out (they carry a header).
    static constexpr bool kNeedFree = true;

    void * Malloc(size_t size);
    void * Realloc(void * original_ptr, size_t original_size, size_t new_size);
    static void Free(void * ptr) noexcept;

    bool operator==(const RapidJSONMemoryTrackerAllocator &) const noexcept { return true; }
    bool operator!=(const RapidJSONMemoryTrackerAllocator &) const noexcept { return false; }
};

}

#endif
