#include <cassert>
#include <cstring>
#include <mutex>
#include <new>
#include <unordered_map>
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
    void * ptr = nullptr;
    try
    {
        ptr = Memory::newImpl(size);
    }
    catch (...)
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        throw;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new(std::size_t size, std::align_val_t align)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = nullptr;
    try
    {
        ptr = Memory::newImpl(size, align);
    }
    catch (...)
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        throw;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr = nullptr;
    try
    {
        ptr = Memory::newImpl(size);
    }
    catch (...)
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        throw;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size, std::align_val_t align)
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = nullptr;
    try
    {
        ptr = Memory::newImpl(size, align);
    }
    catch (...)
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        throw;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr = Memory::newNoExcept(size);
    if (unlikely(!ptr))
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace);
    void * ptr = Memory::newNoExcept(size);
    if (unlikely(!ptr))
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = Memory::newNoExcept(size, align);
    if (unlikely(!ptr))
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void * operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    AllocationTrace trace;
    std::size_t actual_size = Memory::trackMemory(size, trace, align);
    void * ptr = Memory::newNoExcept(size, align);
    if (unlikely(!ptr))
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        return nullptr;
    }
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

namespace
{

thread_local bool c_allocation_tracking_disabled = false;

bool isCAllocationTrackingDisabled()
{
    return c_allocation_tracking_disabled;
}

class CAllocationTrackingBlocker
{
public:
    CAllocationTrackingBlocker()
        : previous_value(c_allocation_tracking_disabled)
    {
        c_allocation_tracking_disabled = true;
    }

    ~CAllocationTrackingBlocker()
    {
        c_allocation_tracking_disabled = previous_value;
    }

private:
    bool previous_value;
};

#if !defined(SANITIZER) && !defined(SANITIZE_COVERAGE)
size_t estimateGetAddrInfoSize(const struct addrinfo * result)
{
    size_t total_size = 0;
    const auto * current = result;
    while (current)
    {
        total_size += sizeof(struct addrinfo);
        total_size += current->ai_addrlen;
        if (current->ai_canonname)
            total_size += std::strlen(current->ai_canonname) + 1;

        current = current->ai_next;
    }

    return total_size;
}

/// Declared before the tracking objects so it is destroyed after them,
/// allowing safe shutdown-time checks against static destruction order issues.
std::atomic_bool getaddrinfo_tracking_initialized{false};

struct GetaddrInfoTracking
{
    std::mutex mutex;
    std::unordered_map<const struct addrinfo *, size_t> sizes;

    GetaddrInfoTracking() { getaddrinfo_tracking_initialized.store(true, std::memory_order_release); }
    ~GetaddrInfoTracking() { getaddrinfo_tracking_initialized.store(false, std::memory_order_release); }
};

GetaddrInfoTracking getaddrinfo_tracking;
#endif

}

extern "C" void * __wrap_malloc(size_t size) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_malloc(size);

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
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_calloc(number_of_members, size);

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
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_realloc(ptr, size);

    AllocationTrace old_trace;
    size_t old_actual_size = 0;
    if (ptr)
    {
        old_actual_size = Memory::untrackMemory(ptr, old_trace);
        old_trace.onFree(ptr, old_actual_size);
    }

    AllocationTrace new_trace;
    size_t new_actual_size = Memory::trackMemoryFromC(size, new_trace);
    void * res = __real_realloc(ptr, size);
    if (unlikely(!res))
    {
        new_trace = CurrentMemoryTracker::free(new_actual_size);
        if (ptr && size != 0)
        {
            old_trace = CurrentMemoryTracker::allocNoThrow(old_actual_size);
            old_trace.onAlloc(ptr, old_actual_size);
        }
        return nullptr;
    }
    new_trace.onAlloc(res, new_actual_size);
    return res;
}

extern "C" int __wrap_posix_memalign(void ** memptr, size_t alignment, size_t size) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_posix_memalign(memptr, alignment, size);

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
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_aligned_alloc(alignment, size);

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
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_valloc(size);

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
    if (unlikely(isCAllocationTrackingDisabled()))
    {
        size_t real_size = 0;
        if (__builtin_mul_overflow(number_of_members, size, &real_size))
            return nullptr;
        return __real_realloc(ptr, real_size);
    }

    size_t real_size = 0;
    if (__builtin_mul_overflow(number_of_members, size, &real_size))
        return nullptr;

    return __wrap_realloc(ptr, real_size);
}

extern "C" void __wrap_free(void * ptr) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
    {
        __real_free(ptr);
        return;
    }

    AllocationTrace trace;
    size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);
    __real_free(ptr);
}

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
extern "C" void * __wrap_memalign(size_t alignment, size_t size) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_memalign(alignment, size);

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
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_pvalloc(size);

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

#if !defined(SANITIZER) && !defined(SANITIZE_COVERAGE)
extern "C" char * __wrap_strdup(const char * str) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_strdup(str);

    CAllocationTrackingBlocker blocker;
    char * res = __real_strdup(str);
    if (unlikely(!res))
        return nullptr;

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(std::strlen(str) + 1, trace);
    trace.onAlloc(res, actual_size);
    return res;
}

extern "C" char * __wrap_strndup(const char * str, size_t size) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_strndup(str, size);

    CAllocationTrackingBlocker blocker;
    char * res = __real_strndup(str, size);
    if (unlikely(!res))
        return nullptr;

    AllocationTrace trace;
    size_t actual_size = Memory::trackMemoryFromC(::strnlen(str, size) + 1, trace);
    trace.onAlloc(res, actual_size);
    return res;
}

extern "C" int __wrap_getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** result) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
        return __real_getaddrinfo(node, service, hints, result);

    int res = 0;
    {
        CAllocationTrackingBlocker blocker;
        res = __real_getaddrinfo(node, service, hints, result);
    }

    if (res != 0 || !result || !*result)
        return res;

    if (likely(getaddrinfo_tracking_initialized.load(std::memory_order_acquire)))
    {
        size_t tracked_size = estimateGetAddrInfoSize(*result);
        AllocationTrace trace = CurrentMemoryTracker::allocNoThrow(static_cast<Int64>(tracked_size));
        trace.onAlloc(*result, tracked_size);

        try
        {
            std::lock_guard lock(getaddrinfo_tracking.mutex);
            getaddrinfo_tracking.sizes[*result] = tracked_size;
        }
        catch (...)
        {
            auto rollback_trace = CurrentMemoryTracker::free(static_cast<Int64>(tracked_size));
            rollback_trace.onFree(*result, tracked_size);
        }
    }

    return res;
}

extern "C" void __wrap_freeaddrinfo(struct addrinfo * result) // NOLINT
{
    if (unlikely(isCAllocationTrackingDisabled()))
    {
        __real_freeaddrinfo(result);
        return;
    }

    size_t tracked_size = 0;
    if (result && likely(getaddrinfo_tracking_initialized.load(std::memory_order_acquire)))
    {
        try
        {
            std::lock_guard lock(getaddrinfo_tracking.mutex);
            auto it = getaddrinfo_tracking.sizes.find(result);
            if (it != getaddrinfo_tracking.sizes.end())
            {
                tracked_size = it->second;
                getaddrinfo_tracking.sizes.erase(it);
            }
        }
        catch (...) // NOLINT(bugprone-empty-catch) - cannot throw from C callback
        {
        }
    }

    {
        CAllocationTrackingBlocker blocker;
        __real_freeaddrinfo(result);
    }

    if (tracked_size)
    {
        auto trace = CurrentMemoryTracker::free(static_cast<Int64>(tracked_size));
        trace.onFree(result, tracked_size);
    }
}
#endif // !defined(SANITIZER) && !defined(SANITIZE_COVERAGE)

#pragma clang diagnostic pop
