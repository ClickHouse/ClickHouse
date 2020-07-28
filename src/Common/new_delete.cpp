#include <common/memory.h>
#include <Common/MemoryTracker.h>

#include <iostream>
#include <new>

#if defined(OS_LINUX)
#   include <malloc.h>
#elif defined(OS_DARWIN)
#   include <malloc/malloc.h>
#endif

#if defined(OS_DARWIN) && defined(BUNDLED_STATIC_JEMALLOC)
extern "C"
{
extern void zone_register();
}

struct InitializeJemallocZoneAllocatorForOSX
{
    InitializeJemallocZoneAllocatorForOSX()
    {
        /// In case of OSX jemalloc register itself as a default zone allocator.
        ///
        /// But when you link statically then zone_register() will not be called,
        /// and even will be optimized out:
        ///
        /// It is ok to call it twice (i.e. in case of shared libraries)
        /// Since zone_register() is a no-op if the default zone is already replaced with something.
        ///
        /// https://github.com/jemalloc/jemalloc/issues/708
        zone_register();
    }
} initializeJemallocZoneAllocatorForOSX;
#endif

/// Replace default new/delete with memory tracking versions.
/// @sa https://en.cppreference.com/w/cpp/memory/new/operator_new
///     https://en.cppreference.com/w/cpp/memory/new/operator_delete

namespace Memory
{

inline ALWAYS_INLINE void trackMemory(std::size_t size)
{
    std::size_t actual_size = size;

#if USE_JEMALLOC && JEMALLOC_VERSION_MAJOR >= 5
    /// The nallocx() function allocates no memory, but it performs the same size computation as the mallocx() function
    /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
    if (likely(size != 0))
        actual_size = nallocx(size, 0);
#endif

    CurrentMemoryTracker::alloc(actual_size);
}

inline ALWAYS_INLINE bool trackMemoryNoExcept(std::size_t size) noexcept
{
    try
    {
        trackMemory(size);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

inline ALWAYS_INLINE void untrackMemory(void * ptr [[maybe_unused]], std::size_t size [[maybe_unused]] = 0) noexcept
{
    try
    {
#if USE_JEMALLOC && JEMALLOC_VERSION_MAJOR >= 5
        /// @note It's also possible to use je_malloc_usable_size() here.
        if (likely(ptr != nullptr))
            CurrentMemoryTracker::free(sallocx(ptr, 0));
#else
        if (size)
            CurrentMemoryTracker::free(size);
#    if defined(_GNU_SOURCE)
        /// It's innaccurate resource free for sanitizers. malloc_usable_size() result is greater or equal to allocated size.
        else
            CurrentMemoryTracker::free(malloc_usable_size(ptr));
#    endif
#endif
    }
    catch (...)
    {}
}

}

/// new

void * operator new(std::size_t size)
{
    Memory::trackMemory(size);
    return Memory::newImpl(size);
}

void * operator new[](std::size_t size)
{
    Memory::trackMemory(size);
    return Memory::newImpl(size);
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    if (likely(Memory::trackMemoryNoExcept(size)))
        return Memory::newNoExept(size);
    return nullptr;
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    if (likely(Memory::trackMemoryNoExcept(size)))
        return Memory::newNoExept(size);
    return nullptr;
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
    Memory::untrackMemory(ptr);
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr) noexcept
{
    Memory::untrackMemory(ptr);
    Memory::deleteImpl(ptr);
}

void operator delete(void * ptr, std::size_t size) noexcept
{
    Memory::untrackMemory(ptr, size);
    Memory::deleteSized(ptr, size);
}

void operator delete[](void * ptr, std::size_t size) noexcept
{
    Memory::untrackMemory(ptr, size);
    Memory::deleteSized(ptr, size);
}
