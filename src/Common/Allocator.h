#pragma once

#include <cstring>

#ifdef NDEBUG
    #define ALLOCATOR_ASLR 0
#else
    #define ALLOCATOR_ASLR 1
#endif

#include <pcg_random.hpp>
#include <Common/thread_local_rng.h>

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
#include <malloc.h>
#endif

#include <cstdlib>
#include <algorithm>
#include <sys/mman.h>

#include <Core/Defines.h>
#if defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
    /// Thread and memory sanitizers do not intercept mremap. The usage of
    /// mremap will lead to false positives.
    #define DISABLE_MREMAP 1
#endif
#include <base/mremap.h>
#include <base/getPageSize.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

#include <Common/Allocator_fwd.h>


static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}

}

/** Previously there was a code which tried to use manual mmap and mremap (clickhouse_mremap.h) for large allocations/reallocations (64MB+).
  * Most modern allocators (including jemalloc) don't use mremap, so the idea was to take advantage from mremap system call for large reallocs.
  * Actually jemalloc had support for mremap, but it was intentionally removed from codebase https://github.com/jemalloc/jemalloc/commit/e2deab7a751c8080c2b2cdcfd7b11887332be1bb.
  * Our performance tests also shows that without manual mmap/mremap/munmap clickhouse is overall faster for about 1-2% and up to 5-7x for some types of queries.
  * That is why we don't do manuall mmap/mremap/munmap here and completely rely on jemalloc for allocations of any size.
  */

/** Responsible for allocating / freeing memory. Used, for example, in PODArray, Arena.
  * Also used in hash tables.
  * The interface is different from std::allocator
  * - the presence of the method realloc, which for large chunks of memory uses mremap;
  * - passing the size into the `free` method;
  * - by the presence of the `alignment` argument;
  * - the possibility of zeroing memory (used in hash tables);
  */
template <bool clear_memory_>
class Allocator
{
public:
    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 0)
    {
        checkSize(size);
        CurrentMemoryTracker::alloc(size);
        return allocNoTrack(size, alignment);
    }

    /// Free memory range.
    void free(void * buf, size_t size)
    {
        try
        {
            checkSize(size);
            freeNoTrack(buf);
            CurrentMemoryTracker::free(size);
        }
        catch (...)
        {
            DB::tryLogCurrentException("Allocator::free");
            throw;
        }
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        checkSize(new_size);

        if (old_size == new_size)
        {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        }
        else if (alignment <= MALLOC_MIN_ALIGNMENT)
        {
            /// Resize malloc'd memory region with no special alignment requirement.
            CurrentMemoryTracker::realloc(old_size, new_size);

            void * new_buf = ::realloc(buf, new_size);
            if (nullptr == new_buf)
            {
                DB::throwFromErrno(
                    fmt::format("Allocator: Cannot realloc from {} to {}.", ReadableSize(old_size), ReadableSize(new_size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
            }

            buf = new_buf;
            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
        }
        else
        {
            /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.
            void * new_buf = alloc(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            free(buf, old_size);
            buf = new_buf;
        }

        return buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }

    static constexpr bool clear_memory = clear_memory_;

private:
    void * allocNoTrack(size_t size, size_t alignment)
    {
        void * buf;
        if (alignment <= MALLOC_MIN_ALIGNMENT)
        {
            if constexpr (clear_memory)
                buf = ::calloc(size, 1);
            else
                buf = ::malloc(size);

            if (nullptr == buf)
                DB::throwFromErrno(fmt::format("Allocator: Cannot malloc {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        else
        {
            buf = nullptr;
            int res = posix_memalign(&buf, alignment, size);

            if (0 != res)
                DB::throwFromErrno(fmt::format("Cannot allocate memory (posix_memalign) {}.", ReadableSize(size)),
                    DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);

            if constexpr (clear_memory)
                memset(buf, 0, size);
        }
        return buf;
    }

    void freeNoTrack(void * buf)
    {
        ::free(buf);
    }

    void checkSize(size_t size)
    {
        /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
        if (size >= 0x8000000000000000ULL)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
    }
};


/** Allocator with optimization to place small memory ranges in automatic memory.
  */
template <typename Base, size_t _initial_bytes, size_t Alignment>
class AllocatorWithStackMemory : private Base
{
private:
    alignas(Alignment) char stack_memory[_initial_bytes];

public:
    static constexpr size_t initial_bytes = _initial_bytes;

    /// Do not use boost::noncopyable to avoid the warning about direct base
    /// being inaccessible due to ambiguity, when derived classes are also
    /// noncopiable (-Winaccessible-base).
    AllocatorWithStackMemory(const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory & operator = (const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory() = default;
    ~AllocatorWithStackMemory() = default;

    void * alloc(size_t size)
    {
        if (size <= initial_bytes)
        {
            if constexpr (Base::clear_memory)
                memset(stack_memory, 0, initial_bytes);
            return stack_memory;
        }

        return Base::alloc(size, Alignment);
    }

    void free(void * buf, size_t size)
    {
        if (size > initial_bytes)
            Base::free(buf, size);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size)
    {
        /// Was in stack_memory, will remain there.
        if (new_size <= initial_bytes)
            return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > initial_bytes)
            return Base::realloc(buf, old_size, new_size, Alignment);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size, Alignment);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return initial_bytes;
    }
};

// A constant that gives the number of initially available bytes in
// the allocator. Used to check that this number is in sync with the
// initial size of array or hash table that uses the allocator.
template<typename TAllocator>
constexpr size_t allocatorInitialBytes = 0;

template<typename Base, size_t initial_bytes, size_t Alignment>
constexpr size_t allocatorInitialBytes<AllocatorWithStackMemory<
    Base, initial_bytes, Alignment>> = initial_bytes;

/// Prevent implicit template instantiation of Allocator

extern template class Allocator<false>;
extern template class Allocator<true>;
