#pragma once

#include <cstddef>
#include <cstdlib>

/// The C allocation functions, spelled portably where POSIX and Windows disagree.
///
/// POSIX allocates over-aligned memory with `posix_memalign`/`aligned_alloc` and releases it
/// with the ordinary `free`. The Windows CRT has neither function; its `_aligned_malloc` must
/// be released with `_aligned_free`, resized with `_aligned_realloc`, and passing such a
/// pointer to plain `free`/`realloc` is undefined. So the platforms differ not only in how
/// memory is allocated but in how it is *released*, and a shim for the allocation side alone
/// would silently corrupt the heap. Hence the whole family lives here, and on Windows all of
/// it - including the non-over-aligned `alignedMalloc`/`alignedCalloc` - goes through the
/// `_aligned_*` functions, so that any pointer these return can be passed to `alignedRealloc`
/// and `alignedFree` regardless of which one produced it.
///
/// Always release with `alignedFree` what this family returned; never mix with `free`.
///
/// `alignment` must be a power of two and a multiple of `sizeof(void *)`.

#if defined(OS_WINDOWS)
#include <cerrno>
#include <cstring>
#include <malloc.h>
#endif

namespace DB
{

/// The alignment `malloc` is guaranteed to give, and the one the non-over-aligned functions
/// below request on Windows so that every pointer in this family is release-compatible.
inline constexpr size_t DEFAULT_MALLOC_ALIGNMENT = alignof(std::max_align_t);

/// `posix_memalign`: returns 0 on success, or a POSIX error number without touching `errno`.
inline int alignedPosixMemalign(void ** out, size_t alignment, size_t size)
{
#if defined(OS_WINDOWS)
    /// `_aligned_malloc` rejects a non-power-of-two alignment with EINVAL, matching
    /// `posix_memalign`, but reports it through `errno` rather than the return value.
    void * ptr = ::_aligned_malloc(size, alignment);
    if (!ptr)
        return errno == EINVAL ? EINVAL : ENOMEM;
    *out = ptr;
    return 0;
#else
    return ::posix_memalign(out, alignment, size);
#endif
}

/// `aligned_alloc`: returns the pointer, or nullptr on failure.
inline void * alignedAlloc(size_t alignment, size_t size)
{
#if defined(OS_WINDOWS)
    return ::_aligned_malloc(size, alignment);
#else
    return ::aligned_alloc(alignment, size);
#endif
}

inline void * alignedMalloc(size_t size)
{
#if defined(OS_WINDOWS)
    return ::_aligned_malloc(size, DEFAULT_MALLOC_ALIGNMENT);
#else
    return ::malloc(size);
#endif
}

inline void * alignedCalloc(size_t nmemb, size_t size)
{
#if defined(OS_WINDOWS)
    /// `_aligned_malloc` has no zeroing counterpart that mingw-w64 provides, so do it by
    /// hand - including the multiplication overflow check that `calloc` owes the caller.
    if (nmemb != 0 && size > static_cast<size_t>(-1) / nmemb)
        return nullptr;
    const size_t bytes = nmemb * size;
    void * ptr = ::_aligned_malloc(bytes, DEFAULT_MALLOC_ALIGNMENT);
    if (ptr)
        ::memset(ptr, 0, bytes);
    return ptr;
#else
    return ::calloc(nmemb, size);
#endif
}

/// Only valid for a pointer that was allocated with `DEFAULT_MALLOC_ALIGNMENT`, i.e. by
/// `alignedMalloc`/`alignedCalloc` - `_aligned_realloc` needs to be told the original
/// alignment and cannot change it.
inline void * alignedRealloc(void * ptr, size_t size)
{
#if defined(OS_WINDOWS)
    return ::_aligned_realloc(ptr, size, DEFAULT_MALLOC_ALIGNMENT);
#else
    return ::realloc(ptr, size);
#endif
}

inline void alignedFree(void * ptr) noexcept
{
#if defined(OS_WINDOWS)
    ::_aligned_free(ptr);
#else
    ::free(ptr);
#endif
}

}
