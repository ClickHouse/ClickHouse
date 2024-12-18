#pragma once

#include <cstring>

#ifdef NDEBUG
    #define ALLOCATOR_ASLR 0
#else
    #define ALLOCATOR_ASLR 1
#endif

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
#include <malloc.h>
#endif

#include <Core/Defines.h>
#include <Common/Allocator_fwd.h>
#include <cstdlib>


extern const size_t POPULATE_THRESHOLD;

static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

/** Previously there was a code which tried to use manual mmap and mremap (clickhouse_mremap.h) for large allocations/reallocations (64MB+).
  * Most modern allocators (including jemalloc) don't use mremap, so the idea was to take advantage from mremap system call for large reallocs.
  * Actually jemalloc had support for mremap, but it was intentionally removed from codebase https://github.com/jemalloc/jemalloc/commit/e2deab7a751c8080c2b2cdcfd7b11887332be1bb.
  * Our performance tests also shows that without manual mmap/mremap/munmap clickhouse is overall faster for about 1-2% and up to 5-7x for some types of queries.
  * That is why we don't do manual mmap/mremap/munmap here and completely rely on jemalloc for allocations of any size.
  */

/** Responsible for allocating / freeing memory. Used, for example, in PODArray, Arena.
  * Also used in hash tables.
  * The interface is different from std::allocator
  * - the presence of the method realloc, which for large chunks of memory uses mremap;
  * - passing the size into the `free` method;
  * - by the presence of the `alignment` argument;
  * - the possibility of zeroing memory (used in hash tables);
  */
template <bool clear_memory_, bool populate>
class Allocator
{
public:
    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 0);

    /// Free memory range.
    void free(void * buf, size_t size);

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0);

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }

    static constexpr bool clear_memory = clear_memory_;

private:
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

extern template class Allocator<false, false>;
extern template class Allocator<true, false>;
extern template class Allocator<false, true>;
extern template class Allocator<true, true>;
