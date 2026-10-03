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

/// Allocator for the stacks rapidjson uses while parsing one document. Blocks come from an inline buffer
/// that `reset` rewinds, so a small document is parsed without heap allocations; blocks that do not fit
/// are allocated with `RapidJSONMemoryTrackerAllocator`.
class RapidJSONStackAllocator /// NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - buffer is arena storage, written before read
{
public:
    static constexpr bool kNeedFree = true;

    RapidJSONStackAllocator() = default; /// NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    RapidJSONStackAllocator(const RapidJSONStackAllocator &) = delete;
    RapidJSONStackAllocator & operator=(const RapidJSONStackAllocator &) = delete;

    void * Malloc(size_t size);
    void * Realloc(void * original_ptr, size_t original_size, size_t new_size);
    static void Free(void * ptr) noexcept;

    /// Only valid while no block from the buffer is in use.
    void reset() { buffer_used = 0; }

private:
    static constexpr size_t buffer_size = 2048;
    alignas(std::max_align_t) char buffer[buffer_size];
    size_t buffer_used = 0;
};

}

#endif
