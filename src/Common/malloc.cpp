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

/// FreeBSD does not declare memalign in its headers.
#if defined(OS_FREEBSD)
void * memalign(size_t alignment, size_t size);
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

/// On glibc, reallocarray is declared with __THROW (noexcept).
/// On FreeBSD and musl, it is declared without noexcept, so we must match.
#if !defined(OS_FREEBSD) && !defined(USE_MUSL)
extern "C" void * reallocarray(void * ptr, size_t nmemb, size_t size) noexcept;
#endif

void * reallocarray(void * ptr, size_t nmemb, size_t size)
#if !defined(OS_FREEBSD) && !defined(USE_MUSL)
    noexcept
#endif
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(nmemb, size, &real_size))
    {
        errno = ENOMEM;
        return nullptr;
    }

    return realloc(ptr, real_size);
}

/// FreeBSD does not declare malloc_usable_size in its headers.
#if defined(OS_FREEBSD)
size_t malloc_usable_size(void * ptr);
#endif

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


#if USE_JEMALLOC && defined(OS_DARWIN)

#include <cstddef>
#include <cstring>
#include <pthread.h>

#include <mach/mach.h>
#include <malloc/malloc.h>
#include <jemalloc/jemalloc.h>

#include <base/getPageSize.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/memory.h>

/// macOS counterpart of the symbol interposition above.
///
/// On Linux/FreeBSD we override the libc allocation symbols so every raw malloc/free is tracked.
/// That does not work on Mach-O: `free` is dispatched through the "malloc zone" that owns the
/// pointer, so overriding the `free` symbol with one that calls `je_free` would corrupt pointers
/// belonging to other zones, and dyld interposition cannot reach the malloc calls libSystem makes
/// internally. Instead we lean on the fact that jemalloc installs itself as the process' default
/// malloc zone (contrib/jemalloc/src/zone.c): every allocation and free flows through that zone's
/// callbacks, so wrapping them tracks the whole process while staying zone-consistent.
///
/// `initializeJemallocZoneMemoryTracking` must run once, after jemalloc has registered its zone.
extern "C" void initializeJemallocZoneMemoryTracking();

namespace
{

/// Original jemalloc callbacks, captured before we overwrite them.
malloc_zone_t saved_zone;

/// Reentrancy guard. macOS allocates thread-local storage lazily, on first access per thread, by
/// calling malloc. Our tracking touches thread-locals (CurrentMemoryTracker), so the first
/// allocation on a new thread would recurse: malloc -> track -> TLV init -> malloc -> ... until the
/// stack overflows. We detect the reentry with a pthread key, whose thread-specific slot lives in a
/// preallocated array and never itself calls malloc, and pass such allocations straight through
/// untracked. This is macOS-specific: on Linux TLS is set up at thread creation, not lazily.
///
/// Known limitation: the pass-through allocation (the thread's ~40 KiB dyld TLV block) is charged
/// to no tracker, but its eventual free runs the normal tracked path and subtracts it. Charging it
/// is impossible - MemoryTracker::allocImpl itself reads thread-locals, so it would re-enter the
/// same lazy TLV init. The effect is one ~40 KiB block per thread: a static under-count while the
/// thread lives, and a ~40 KiB downward drift of the *global* tracker per completed thread
/// create->destroy cycle. It does not affect per-query max_memory_usage (TLV frees happen at thread
/// teardown, charged to the global tracker, not a query's). Measured on a server that spun up ~865
/// threads: ~35 MiB total, and no drift was realized under 1000-connection churn because pooled
/// threads persist. Accepted as a bounded, macOS-only, dev-platform limitation rather than paying a
/// per-free pointer-set lookup on the allocation hot path.
pthread_key_t reentrancy_key;

struct ReentrancyGuard
{
    bool engaged;

    ReentrancyGuard()
        : engaged(pthread_getspecific(reentrancy_key) == nullptr)
    {
        if (engaged)
            pthread_setspecific(reentrancy_key, reinterpret_cast<void *>(1));
    }

    ~ReentrancyGuard()
    {
        if (engaged)
            pthread_setspecific(reentrancy_key, nullptr);
    }
};

/// Credit the tracker for a pointer that is about to be freed. The size comes from the zone's own
/// `size` callback (jemalloc's ivsalloc), which returns 0 for pointers jemalloc does not own.
/// Darwin routes foreign pointers (e.g. from setenv) through these callbacks, and je_sallocx is
/// UB on them, so we must use the ownership-aware zone size and skip untracking when it is 0.
void untrackZonePointer(malloc_zone_t * zone, void * ptr)
{
    size_t actual_size = saved_zone.size(zone, ptr);
    if (actual_size == 0)
        return;
    AllocationTrace trace = CurrentMemoryTracker::free(actual_size);
    trace.onFree(ptr, actual_size);
}

extern "C"
{

static void * trackedZoneMalloc(malloc_zone_t * zone, size_t size)
{
    ReentrancyGuard guard;
    if (!guard.engaged) [[unlikely]]
        return saved_zone.malloc(zone, size);

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * ptr = saved_zone.malloc(zone, size);
    if (ptr == nullptr) [[unlikely]]
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

static void * trackedZoneCalloc(malloc_zone_t * zone, size_t num, size_t size)
{
    ReentrancyGuard guard;
    if (!guard.engaged) [[unlikely]]
        return saved_zone.calloc(zone, num, size);

    size_t real_size = 0;
    if (__builtin_mul_overflow(num, size, &real_size))
        real_size = 0;

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(real_size, trace);
    void * ptr = saved_zone.calloc(zone, num, size);
    if (ptr == nullptr) [[unlikely]]
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

static void * trackedZoneValloc(malloc_zone_t * zone, size_t size)
{
    ReentrancyGuard guard;
    if (!guard.engaged) [[unlikely]]
        return saved_zone.valloc(zone, size);

    /// valloc returns page-aligned memory (jemalloc's zone_valloc uses posix_memalign with PAGE),
    /// so charge the page-aligned size class to stay balanced with je_sallocx on free.
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(getPageSize()));
    void * ptr = saved_zone.valloc(zone, size);
    if (ptr == nullptr) [[unlikely]]
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

static void * trackedZoneMemalign(malloc_zone_t * zone, size_t alignment, size_t size)
{
    ReentrancyGuard guard;
    if (!guard.engaged) [[unlikely]]
        return saved_zone.memalign(zone, alignment, size);

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(alignment));
    void * ptr = saved_zone.memalign(zone, alignment, size);
    if (ptr == nullptr) [[unlikely]]
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

static void * trackedZoneRealloc(malloc_zone_t * zone, void * ptr, size_t size)
{
    ReentrancyGuard guard;
    if (!guard.engaged) [[unlikely]]
        return saved_zone.realloc(zone, ptr, size);

    /// Ownership-aware size; 0 if ptr is not jemalloc-owned (je_sallocx would be UB on it).
    size_t old_actual_size = ptr ? saved_zone.size(zone, ptr) : 0;
    void * res = saved_zone.realloc(zone, ptr, size);

    /// realloc consumes ptr only on success, or when size == 0 (jemalloc's zero_realloc:free frees
    /// ptr and returns nullptr). A nullptr result for a non-zero size is a failure: ptr is still
    /// live and owned by the caller, so it must stay tracked - a later free will untrack it.
    if (old_actual_size != 0 && (res != nullptr || size == 0))
    {
        AllocationTrace free_trace = CurrentMemoryTracker::free(old_actual_size);
        free_trace.onFree(ptr, old_actual_size);
    }

    /// Charge the new block only if jemalloc owns it. A foreign pointer reallocs through the
    /// system allocator into another foreign block (size 0 here), which must not be charged or the
    /// bytes would never be credited back by trackedZoneFree. Mirrors the plain-alloc wrappers.
    if (res != nullptr && saved_zone.size(zone, res) != 0)
    {
        AllocationTrace alloc_trace;
        size_t new_actual_size = Memory::trackMemoryFromC(size, alloc_trace);
        alloc_trace.onAlloc(res, new_actual_size);
    }
    return res;
}

static void trackedZoneFree(malloc_zone_t * zone, void * ptr)
{
    ReentrancyGuard guard;
    if (guard.engaged)
        untrackZonePointer(zone, ptr);
    saved_zone.free(zone, ptr);
}

static void trackedZoneFreeDefiniteSize(malloc_zone_t * zone, void * ptr, size_t size)
{
    ReentrancyGuard guard;
    if (guard.engaged)
        untrackZonePointer(zone, ptr);
    saved_zone.free_definite_size(zone, ptr, size);
}

static unsigned trackedZoneBatchMalloc(malloc_zone_t * zone, size_t size, void ** results, unsigned num_requested)
{
    unsigned num = saved_zone.batch_malloc(zone, size, results, num_requested);
    ReentrancyGuard guard;
    if (guard.engaged)
    {
        for (unsigned i = 0; i < num; ++i)
        {
            AllocationTrace trace;
            size_t actual_size = Memory::trackMemoryFromC(size, trace);
            trace.onAlloc(results[i], actual_size);
        }
    }
    return num;
}

static void trackedZoneBatchFree(malloc_zone_t * zone, void ** to_be_freed, unsigned num)
{
    {
        ReentrancyGuard guard;
        if (guard.engaged)
        {
            for (unsigned i = 0; i < num; ++i)
            {
                if (to_be_freed[i] != nullptr)
                    untrackZonePointer(zone, to_be_freed[i]);
            }
        }
    }
    saved_zone.batch_free(zone, to_be_freed, num);
}

} // extern "C"

}

extern "C" void initializeJemallocZoneMemoryTracking()
{
    static bool installed = false;
    if (installed)
        return;

    /// Find jemalloc's registered zone by name. We must not rely on malloc_default_zone(): on
    /// modern macOS it returns a helper zone that merely dispatches to the real default, and our
    /// free/realloc wrappers size pointers with je_sallocx, which is only valid for jemalloc's
    /// own allocations.
    vm_address_t * zones = nullptr;
    unsigned count = 0;
    if (malloc_get_all_zones(mach_task_self(), nullptr, &zones, &count) != KERN_SUCCESS)
        return;

    malloc_zone_t * zone = nullptr;
    for (unsigned i = 0; i < count; ++i)
    {
        auto * candidate = reinterpret_cast<malloc_zone_t *>(zones[i]);
        const char * name = malloc_get_zone_name(candidate);
        if (name != nullptr && std::strcmp(name, "jemalloc_zone") == 0)
        {
            zone = candidate;
            break;
        }
    }
    if (zone == nullptr)
        return;

    /// Must exist before the wrappers (which read it) become reachable.
    if (pthread_key_create(&reentrancy_key, nullptr) != 0)
        return;
    installed = true;

    /// jemalloc's zone is its own mutable static (it writes the struct in zone_init and keeps
    /// mutating it), so the page is writable; no mprotect dance is needed.
    saved_zone = *zone;

    zone->malloc = trackedZoneMalloc;
    zone->calloc = trackedZoneCalloc;
    zone->valloc = trackedZoneValloc;
    zone->realloc = trackedZoneRealloc;
    zone->free = trackedZoneFree;
    if (saved_zone.memalign != nullptr)
        zone->memalign = trackedZoneMemalign;
    if (saved_zone.free_definite_size != nullptr)
        zone->free_definite_size = trackedZoneFreeDefiniteSize;
    if (saved_zone.batch_malloc != nullptr)
        zone->batch_malloc = trackedZoneBatchMalloc;
    if (saved_zone.batch_free != nullptr)
        zone->batch_free = trackedZoneBatchFree;
}

#endif // USE_JEMALLOC && OS_DARWIN
