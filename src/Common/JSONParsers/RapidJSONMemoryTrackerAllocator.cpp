#include <Common/JSONParsers/RapidJSONMemoryTrackerAllocator.h>

#if USE_RAPIDJSON

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

}

#endif
