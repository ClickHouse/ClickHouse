#pragma once

#include "config.h"

#if USE_RAPIDJSON

#include <cstddef>

#include <Common/Allocator.h>

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

    void * Malloc(size_t size)
    {
        if (size == 0)
            return nullptr;

        char * base = static_cast<char *>(Allocator<false>().alloc(header_size + size));
        *reinterpret_cast<size_t *>(base) = size;
        return base + header_size;
    }

    void * Realloc(void * original_ptr, size_t /*original_size*/, size_t new_size)
    {
        if (new_size == 0)
        {
            Free(original_ptr);
            return nullptr;
        }

        if (original_ptr == nullptr)
            return Malloc(new_size);

        char * base = static_cast<char *>(original_ptr) - header_size;
        const size_t old_size = *reinterpret_cast<size_t *>(base);

        char * new_base
            = static_cast<char *>(Allocator<false>().realloc(base, header_size + old_size, header_size + new_size));
        *reinterpret_cast<size_t *>(new_base) = new_size;
        return new_base + header_size;
    }

    static void Free(void * ptr) noexcept
    {
        if (ptr == nullptr)
            return;

        char * base = static_cast<char *>(ptr) - header_size;
        const size_t size = *reinterpret_cast<size_t *>(base);
        Allocator<false>().free(base, header_size + size);
    }

    bool operator==(const RapidJSONMemoryTrackerAllocator &) const noexcept { return true; }
    bool operator!=(const RapidJSONMemoryTrackerAllocator &) const noexcept { return false; }

private:
    /// `DB::Allocator` aligns the block base to at least MALLOC_MIN_ALIGNMENT; keep the header that
    /// size so the payload handed to rapidjson keeps the same alignment.
    static constexpr size_t header_size = MALLOC_MIN_ALIGNMENT >= sizeof(size_t) ? MALLOC_MIN_ALIGNMENT : sizeof(size_t);
};

}

#endif
