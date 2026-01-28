#include <cassert>
#include <new>
#include "config.h"

#include <Common/memory.h>
#include <Common/AllocationInterceptors.h>

#if defined(OS_DARWIN) && (USE_JEMALLOC)
/// In case of OSX jemalloc register itself as a default zone allocator.
///
/// Sure jemalloc will register itself, since zone_register() declared with
/// constructor attribute (since zone_register is also forbidden from
/// optimizing out), however those constructors will be called before
/// constructors for global variable initializers (__cxx_global_var_init()).
///
/// So to make jemalloc under OSX more stable, we will call it explicitly from
/// global variable initializers so that each allocation will use it.
/// (NOTE: It is ok to call it twice, since zone_register() is a no-op if the
/// default zone is already replaced with something.)
///
/// Refs: https://github.com/jemalloc/jemalloc/issues/708

extern "C"
{
    extern void zone_register();
}

static struct InitializeJemallocZoneAllocatorForOSX
{
    InitializeJemallocZoneAllocatorForOSX()
    {
        zone_register();
        /// jemalloc() initializes itself only on malloc()
        /// and so if some global initializer will have free(nullptr)
        /// jemalloc may trigger some internal assertion.
        ///
        /// To prevent this, we explicitly call malloc(free()) here.
        if (void * ptr = malloc(0))
        {
            free(ptr);
        }
    }
} initializeJemallocZoneAllocatorForOSX;
#endif

/// Replace default new/delete with memory tracking versions.
/// @sa https://en.cppreference.com/w/cpp/memory/new/operator_new
///     https://en.cppreference.com/w/cpp/memory/new/operator_delete

/// new

void * operator new(std::size_t size)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr = Memory::newImpl(size);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new(std::size_t size, std::align_val_t align)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = Memory::newImpl(size, align);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr =  Memory::newImpl(size);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size, std::align_val_t align)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = Memory::newImpl(size, align);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr = Memory::newNoExcept(size);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr = Memory::newNoExcept(size);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = Memory::newNoExcept(size, align);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = Memory::newNoExcept(size, align);
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

/// delete

/// C++17 std 21.6.2.1 (11)
/// If a function without a size parameter is defined, the program should also define the corresponding function with a size parameter.
/// If a function with a size parameter is defined, the program shall also define the corresponding version without the size parameter.

/// cppreference:
/// It's unspecified whether size-aware or size-unaware version is called when deleting objects of
/// incomplete type and arrays of non-class and trivially-destructible class types.


void operator delete(void * ptr) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);
    Memory::deleteImpl(ptr);
}

void operator delete(void * ptr, std::align_val_t align) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace, 0, align);
    trace.onFree(ptr, actual_size);
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr, std::align_val_t align) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace, 0, align);
    trace.onFree(ptr, actual_size);
    Memory::deleteImpl(ptr);
}

void operator delete(void * ptr, std::size_t size) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace, size);
    trace.onFree(ptr, actual_size);
    Memory::deleteSized(ptr, size);
}

void operator delete(void * ptr, std::size_t size, std::align_val_t align) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace, size, align);
    trace.onFree(ptr, actual_size);
    Memory::deleteSized(ptr, size, align);
}

void operator delete[](void * ptr, std::size_t size) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace, size);
    trace.onFree(ptr, actual_size);
    Memory::deleteSized(ptr, size);
}

void operator delete[](void * ptr, std::size_t size, std::align_val_t align) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::untrackMemory(ptr, trace, size, align);
    trace.onFree(ptr, actual_size);
    Memory::deleteSized(ptr, size, align);
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"

extern "C" void * __wrap_malloc(size_t size) // NOLINT
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * ptr = __real_malloc(size);
    if (unlikely(!ptr))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

extern "C" void * __wrap_calloc(size_t number_of_members, size_t size) // NOLINT
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(number_of_members, size, &real_size))
        return nullptr;

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(real_size, trace);
    void * res = __real_calloc(number_of_members, size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}

extern "C" void * __wrap_realloc(void * ptr, size_t size) // NOLINT
{
    if (ptr)
    {
        AllocationTrace trace;
        size_t actual_size = Memory::untrackMemory(ptr, trace);
        trace.onFree(ptr, actual_size);
    }
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * res = __real_realloc(ptr, size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}

extern "C" int __wrap_posix_memalign(void ** memptr, size_t alignment, size_t size) // NOLINT
{
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(alignment));
    int res = __real_posix_memalign(memptr, alignment, size);
    if (unlikely(res != 0))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return res;
    }
    trace.onAlloc(*memptr, actual_size);
    return res;
}

extern "C" void * __wrap_aligned_alloc(size_t alignment, size_t size) // NOLINT
{
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(alignment));
    void * res = __real_aligned_alloc(alignment, size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}

#if !defined(OS_FREEBSD)
extern "C" void * __wrap_valloc(size_t size) // NOLINT
{
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * res = __real_valloc(size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}
#endif

extern "C" void * __wrap_reallocarray(void * ptr, size_t number_of_members, size_t size) // NOLINT
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(number_of_members, size, &real_size))
        return nullptr;

    return __wrap_realloc(ptr, real_size);
}

extern "C" void __wrap_free(void * ptr) // NOLINT
{
    AllocationTrace trace;
    size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);
    __real_free(ptr);
}

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
extern "C" void * __wrap_memalign(size_t alignment, size_t size) // NOLINT
{
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace, static_cast<std::align_val_t>(alignment));
    void * res = __real_memalign(alignment, size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}
#endif

#if !defined(USE_MUSL) && defined(OS_LINUX)
extern "C" void * __wrap_pvalloc(size_t size) // NOLINT
{
    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(size, trace);
    void * res = __real_pvalloc(size);
    if (unlikely(!res))
    {
        trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(res, actual_size);
    return res;
}
#endif

#pragma clang diagnostic pop
