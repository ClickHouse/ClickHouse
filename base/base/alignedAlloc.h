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
///
/// A request for zero bytes is served with a one-byte block on Windows, because `_aligned_malloc`
/// fails for size zero, while `malloc(0)` and `operator new(0)` must succeed on any platform.
///
/// On Windows the functions are mingw-w64's own `__mingw_aligned_*` rather than the CRT's
/// `_aligned_*`. The import library `libmsvcrt.a` forwards `_aligned_malloc` to the C runtime DLL
/// but implements `_aligned_msize` itself, assuming the block layout of `__mingw_aligned_malloc`;
/// the DLL's layout differs (Wine's `_aligned_malloc` keeps a different overhead), so the size
/// read back would not match the one requested. Using one implementation for the whole family
/// keeps the layout and the size query consistent with each other.

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
    void * ptr = ::__mingw_aligned_malloc(size ? size : 1, alignment);
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
    return ::__mingw_aligned_malloc(size ? size : 1, alignment);
#else
    return ::aligned_alloc(alignment, size);
#endif
}

inline void * alignedMalloc(size_t size)
{
#if defined(OS_WINDOWS)
    return ::__mingw_aligned_malloc(size ? size : 1, DEFAULT_MALLOC_ALIGNMENT);
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
    void * ptr = ::__mingw_aligned_malloc(bytes ? bytes : 1, DEFAULT_MALLOC_ALIGNMENT);
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
    /// The third parameter is the alignment, although mingw-w64's header calls it `_Offset`.
    return ::__mingw_aligned_realloc(ptr, size, DEFAULT_MALLOC_ALIGNMENT);
#else
    return ::realloc(ptr, size);
#endif
}

inline void alignedFree(void * ptr) noexcept
{
#if defined(OS_WINDOWS)
    ::__mingw_aligned_free(ptr);
#else
    ::free(ptr);
#endif
}

#if defined(OS_WINDOWS)
/// The size of the block `ptr` points to, as it was requested from `__mingw_aligned_malloc` (a request
/// for zero bytes counts as one, see above). `alignment` must be the one it was allocated with.
/// This is the Windows counterpart of `malloc_usable_size`, which memory tracking uses to learn
/// the size freed by an unsized `operator delete`.
inline size_t alignedAllocationSize(void * ptr, size_t alignment) noexcept
{
    return ::__mingw_aligned_msize(ptr, alignment, 0);
}
#endif

}
