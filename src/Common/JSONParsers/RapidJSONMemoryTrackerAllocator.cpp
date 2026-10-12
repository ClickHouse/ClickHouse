#include <Common/JSONParsers/RapidJSONMemoryTrackerAllocator.h>

#if USE_RAPIDJSON

#include <cstring>
#include <limits>

#include <Common/Allocator.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

namespace
{

/// `DB::Allocator` aligns the block base to at least MALLOC_MIN_ALIGNMENT; keep the header that
/// size so the payload handed to rapidjson keeps the same alignment.
constexpr size_t header_size = MALLOC_MIN_ALIGNMENT >= sizeof(size_t) ? MALLOC_MIN_ALIGNMENT : sizeof(size_t);

/// Size of the block including the header, refusing requests so large that adding the header would
/// wrap around (which would otherwise allocate a tiny block and hand out a pointer past its end).
/// Such a request cannot be satisfied anyway.
size_t withHeader(size_t size)
{
    if (size > std::numeric_limits<size_t>::max() - header_size)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate {} bytes for rapidjson: size is too large", size);
    return header_size + size;
}

/// Heap blocks store their non-zero size in the header, inline blocks of `RapidJSONStackAllocator` store 0.
static_assert(header_size == alignof(std::max_align_t));

bool isInlineBlock(const void * ptr)
{
    return *reinterpret_cast<const size_t *>(static_cast<const char *>(ptr) - header_size) == 0;
}

}

void * RapidJSONMemoryTrackerAllocator::Malloc(size_t size)
{
    if (size == 0)
        return nullptr;

    char * base = static_cast<char *>(Allocator<false>().alloc(withHeader(size)));
    *reinterpret_cast<size_t *>(base) = size;
    return base + header_size;
}

void * RapidJSONMemoryTrackerAllocator::Realloc(void * original_ptr, size_t /*original_size*/, size_t new_size)
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

    char * new_base = static_cast<char *>(Allocator<false>().realloc(base, withHeader(old_size), withHeader(new_size)));
    *reinterpret_cast<size_t *>(new_base) = new_size;
    return new_base + header_size;
}

void RapidJSONMemoryTrackerAllocator::Free(void * ptr) noexcept
{
    if (ptr == nullptr)
        return;

    char * base = static_cast<char *>(ptr) - header_size;
    const size_t size = *reinterpret_cast<size_t *>(base);
    Allocator<false>().free(base, header_size + size);
}

void * RapidJSONStackAllocator::Malloc(size_t size)
{
    if (size == 0)
        return nullptr;

    if (size <= buffer_size)
    {
        const size_t block_size = header_size + (size + header_size - 1) / header_size * header_size;
        if (block_size <= buffer_size - buffer_used)
        {
            char * base = buffer + buffer_used;
            *reinterpret_cast<size_t *>(base) = 0;
            buffer_used += block_size;
            return base + header_size;
        }
    }

    return RapidJSONMemoryTrackerAllocator().Malloc(size);
}

void * RapidJSONStackAllocator::Realloc(void * original_ptr, size_t original_size, size_t new_size)
{
    if (original_ptr == nullptr)
        return Malloc(new_size);

    if (!isInlineBlock(original_ptr))
        return RapidJSONMemoryTrackerAllocator().Realloc(original_ptr, original_size, new_size);

    if (new_size == 0)
        return nullptr;
    if (new_size <= original_size)
        return original_ptr;

    void * new_ptr = Malloc(new_size);
    memcpy(new_ptr, original_ptr, original_size);
    return new_ptr;
}

void RapidJSONStackAllocator::Free(void * ptr) noexcept
{
    if (ptr != nullptr && !isInlineBlock(ptr))
        RapidJSONMemoryTrackerAllocator::Free(ptr);
}

}

#endif
