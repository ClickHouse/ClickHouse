#include <new>

#include <common/likely.h>
#include <Common/MemoryTracker.h>

/// Replace default new/delete with memory tracking versions.
/// @sa https://en.cppreference.com/w/cpp/memory/new/operator_new
///     https://en.cppreference.com/w/cpp/memory/new/operator_delete
#if 1

void * operator new (std::size_t size)
{
    CurrentMemoryTracker::alloc(size);

    auto * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    CurrentMemoryTracker::free(size);

    /// @note no std::get_new_handler logic implemented
    std::__throw_bad_alloc();
}

/// Called instead of 'delete(void * ptr)' if a user-defined replacement is provided
void operator delete (void * ptr, std::size_t size) noexcept
{
    CurrentMemoryTracker::free(size);

    if (likely(ptr != nullptr))
        free(ptr);
}

#endif
