#include <new>

#include <common/memory.h>
#include <Common/MemoryTracker.h>

/// Replace default new/delete with memory tracking versions.
/// @sa https://en.cppreference.com/w/cpp/memory/new/operator_new
///     https://en.cppreference.com/w/cpp/memory/new/operator_delete
#if 1

/// new

void * operator new(std::size_t size)
{
    CurrentMemoryTracker::alloc(size);
    return Memory::newImpl(size);
}

void * operator new[](std::size_t size)
{
    CurrentMemoryTracker::alloc(size);
    return Memory::newImpl(size);
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    CurrentMemoryTracker::alloc(size);
    return Memory::newNoExept(size);
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    CurrentMemoryTracker::alloc(size);
    return Memory::newNoExept(size);
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
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr) noexcept
{
    Memory::deleteImpl(ptr);
}

void operator delete(void * ptr, std::size_t size) noexcept
{
    CurrentMemoryTracker::free(size);
    Memory::deleteSized(ptr, size);
}

void operator delete[](void * ptr, std::size_t size) noexcept
{
    CurrentMemoryTracker::free(size);
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
