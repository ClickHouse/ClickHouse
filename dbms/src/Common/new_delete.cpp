#include <new>

#include <common/memory.h>
#include <Common/MemoryTracker.h>

/// Replace default new/delete with memory tracking versions.
/// @sa https://en.cppreference.com/w/cpp/memory/new/operator_new
///     https://en.cppreference.com/w/cpp/memory/new/operator_delete
#if 1

namespace Memory
{

ALWAYS_INLINE void * trackMemory(void * ptr [[maybe_unused]], std::size_t size [[maybe_unused]]) noexcept
{
#ifdef USE_JEMALLOC
    if (likely(ptr != nullptr))
        CurrentMemoryTracker::alloc(sallocx(ptr, 0));
#else
    CurrentMemoryTracker::alloc(size);
#endif

    return ptr;
}

ALWAYS_INLINE void untrackMemory(void * ptr [[maybe_unused]]) noexcept
{
#ifdef USE_JEMALLOC
    if (likely(ptr != nullptr))
        CurrentMemoryTracker::free(sallocx(ptr, 0));
#endif
}

ALWAYS_INLINE void untrackMemory(void * ptr [[maybe_unused]], std::size_t size [[maybe_unused]]) noexcept
{
#ifdef USE_JEMALLOC
    if (likely(ptr != nullptr))
        CurrentMemoryTracker::free(sallocx(ptr, 0));
#else
    CurrentMemoryTracker::free(size);
#endif
}

}

/// new

void * operator new(std::size_t size)
{
    return Memory::trackMemory(Memory::newImpl(size), size);
}

void * operator new[](std::size_t size)
{
    return Memory::trackMemory(Memory::newImpl(size), size);
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    return Memory::trackMemory(Memory::newNoExept(size), size);
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    return Memory::trackMemory(Memory::newNoExept(size), size);
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

#else

/// new

void * operator new(std::size_t size) { return Memory::newImpl(size); }
void * operator new[](std::size_t size) { return Memory::newImpl(size); }

void * operator new(std::size_t size, const std::nothrow_t &) noexcept { return Memory::newNoExept(size); }
void * operator new[](std::size_t size, const std::nothrow_t &) noexcept { return Memory::newNoExept(size); }

/// delete

void operator delete(void * ptr) noexcept { Memory::deleteImpl(ptr); }
void operator delete[](void * ptr) noexcept { Memory::deleteImpl(ptr); }

void operator delete(void * ptr, const std::nothrow_t &) noexcept { Memory::deleteImpl(ptr); }
void operator delete[](void * ptr, const std::nothrow_t &) noexcept { Memory::deleteImpl(ptr); }

void operator delete(void * ptr, std::size_t size) noexcept { Memory::deleteSized(ptr, size); }
void operator delete[](void * ptr, std::size_t size) noexcept { Memory::deleteSized(ptr, size); }

#endif
