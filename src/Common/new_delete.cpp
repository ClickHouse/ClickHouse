#include <cassert>
#include <new>
#include <Common/config.h>
#include <Common/memory.h>

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
    Memory::trackMemory(size);
    return Memory::newImpl(size);
}

void * operator new(std::size_t size, std::align_val_t align)
{
    Memory::trackMemory(size, align);
    return Memory::newImpl(size, align);
}

void * operator new[](std::size_t size)
{
    Memory::trackMemory(size);
    return Memory::newImpl(size);
}

void * operator new[](std::size_t size, std::align_val_t align)
{
    Memory::trackMemory(size, align);
    return Memory::newImpl(size, align);
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    Memory::trackMemory(size);
    return Memory::newNoExept(size);
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    Memory::trackMemory(size);
    return Memory::newNoExept(size);
}

void * operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    Memory::trackMemory(size, align);
    return Memory::newNoExept(size, align);
}

void * operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept
{
    Memory::trackMemory(size, align);
    return Memory::newNoExept(size, align);
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

void operator delete(void * ptr, std::align_val_t align) noexcept
{
    Memory::untrackMemory(ptr, 0, align);
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr) noexcept
{
    Memory::untrackMemory(ptr);
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr, std::align_val_t align) noexcept
{
    Memory::untrackMemory(ptr, 0, align);
    Memory::deleteImpl(ptr);
}

void operator delete(void * ptr, std::size_t size) noexcept
{
    Memory::untrackMemory(ptr, size);
    Memory::deleteSized(ptr, size);
}

void operator delete(void * ptr, std::size_t size, std::align_val_t align) noexcept
{
    Memory::untrackMemory(ptr, size, align);
    Memory::deleteSized(ptr, size, align);
}

void operator delete[](void * ptr, std::size_t size) noexcept
{
    Memory::untrackMemory(ptr, size);
    Memory::deleteSized(ptr, size);
}

void operator delete[](void * ptr, std::size_t size, std::align_val_t align) noexcept
{
    Memory::untrackMemory(ptr, size, align);
    Memory::deleteSized(ptr, size, align);
}
