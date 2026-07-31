#include "config.h"

#if USE_JEMALLOC && (defined(OS_LINUX) || defined(OS_FREEBSD))

#include <cerrno>
#include <cstdlib>
#include <cstddef>

#include <jemalloc/jemalloc.h>

#include <Common/memory.h>
#include <base/getPageSize.h>

/// Define standard allocation functions that perform memory tracking
/// and delegate to jemalloc via je_* prefixed functions.
///
/// This replaces the previous --wrap linker approach with direct symbol
/// interposition: malloc.cpp compiles to a dedicated object (clickhouse_malloc.o)
/// that appears before libjemalloc.a in the link order.
///
/// jemalloc with je_ prefix does not export valloc/memalign/pvalloc,
/// so we implement them via je_posix_memalign / je_aligned_alloc.

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"

extern "C"
{

void * malloc(size_t size)
{
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * ptr = je_malloc(size);
    if (unlikely(!ptr))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void free(void * ptr)
{
    AllocationTrace trace;
    size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);
    je_free(ptr);
}

void * calloc(size_t nmemb, size_t size)
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(nmemb, size, &real_size))
    {
        errno = ENOMEM;
        return nullptr;
    }

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(real_size, trace);
    void * ptr = je_calloc(nmemb, size);
    if (unlikely(!ptr))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * realloc(void * ptr, size_t size)
{
    size_t old_actual_size = 0;
    if (ptr)
        old_actual_size = je_sallocx(ptr, 0);

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * res = je_realloc(ptr, size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        /// With jemalloc opt.zero_realloc:free (the default), realloc(ptr, 0)
        /// frees ptr and returns nullptr. That is not a failure — untrack the old allocation.
        if (size == 0 && ptr)
        {
            AllocationTrace free_trace = CurrentMemoryTracker::free(old_actual_size);
            free_trace.onFree(ptr, old_actual_size);
        }
        return nullptr;
    }

    if (ptr)
    {
        AllocationTrace free_trace = CurrentMemoryTracker::free(old_actual_size);
        free_trace.onFree(ptr, old_actual_size);
    }
    trace.onAlloc(res, actual_size);
    return res;
}

int posix_memalign(void ** memptr, size_t alignment, size_t size)
{
    if (alignment < sizeof(void *) || (alignment & (alignment - 1)) != 0)
        return EINVAL;
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(alignment));
    int res = je_posix_memalign(memptr, alignment, size);
    if (unlikely(res != 0))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return res;
    }
    trace.onAlloc(*memptr, actual_size);
    return res;
}

void * aligned_alloc(size_t alignment, size_t size)
{
    if (alignment == 0 || (alignment & (alignment - 1)) != 0 || (size % alignment) != 0)
    {
        errno = EINVAL;
        return nullptr;
    }
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(alignment));
    void * res = je_aligned_alloc(alignment, size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}

#if !defined(OS_FREEBSD)
void * valloc(size_t size)
{
    void * res = nullptr;
    size_t page_size = getPageSize();
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(page_size));
    int err = je_posix_memalign(&res, page_size, size);
    if (unlikely(err != 0))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        errno = err;
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}
#endif

void * memalign(size_t alignment, size_t size)
{
    void * res = nullptr;
    if (alignment == 0 || (alignment & (alignment - 1)) != 0)
    {
        errno = EINVAL;
        return nullptr;
    }
    /// posix_memalign requires alignment >= sizeof(void*); widen valid small powers of two.
    size_t effective_alignment = alignment < sizeof(void *) ? sizeof(void *) : alignment;
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(effective_alignment));
    int err = je_posix_memalign(&res, effective_alignment, size);
    if (unlikely(err != 0))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        errno = err;
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}

#if !defined(USE_MUSL) && defined(OS_LINUX)
void * pvalloc(size_t size)
{
    void * res = nullptr;
    size_t page_size = getPageSize();
    size_t rounded_size = size + page_size - 1;
    if (unlikely(rounded_size < size))
    {
        errno = ENOMEM;
        return nullptr;
    }
    rounded_size = rounded_size / page_size * page_size;
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(rounded_size, trace, static_cast<std::align_val_t>(page_size));
    int err = je_posix_memalign(&res, page_size, rounded_size);
    if (unlikely(err != 0))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        errno = err;
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}
#endif

void * reallocarray(void * ptr, size_t nmemb, size_t size)
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(nmemb, size, &real_size))
    {
        errno = ENOMEM;
        return nullptr;
    }

    return realloc(ptr, real_size);
}

size_t malloc_usable_size(void * ptr)
{
    return je_malloc_usable_size(ptr);
}

/// On Linux (non-musl), glibc internally calls __libc_malloc etc.
/// We provide aliases so those internal calls also go through our wrappers.
#if !defined(USE_MUSL) && defined(OS_LINUX)

void * __libc_malloc(size_t size) __attribute__((alias("malloc"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
void __libc_free(void * ptr) __attribute__((alias("free"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
void * __libc_calloc(size_t nmemb, size_t size) __attribute__((alias("calloc"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
void * __libc_realloc(void * ptr, size_t size) __attribute__((alias("realloc"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
void * __libc_memalign(size_t alignment, size_t size) __attribute__((alias("memalign"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
void * __libc_valloc(size_t size) __attribute__((alias("valloc"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
void * __libc_pvalloc(size_t size) __attribute__((alias("pvalloc"))); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)

#endif

} // extern "C"

#pragma clang diagnostic pop

#endif // USE_JEMALLOC && (OS_LINUX || OS_FREEBSD)
