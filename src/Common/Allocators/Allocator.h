#pragma once

#include <string.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#include <cstdlib>
#include <algorithm>
#include <sys/mman.h>

#include <Core/Defines.h>

#include <common/mremap.h>

#include "IGrabberAllocator.h"


static constexpr size_t MMAP_MIN_ALIGNMENT = 4096;
static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int CANNOT_MUNMAP;
extern const int CANNOT_MREMAP;
}

/**
 * @brief Responsible for allocating / freeing memory. Used e.g. in PODArray, Arena, and hash tables.
 *
 * The interface is different from std::allocator:
 *
 * - the presence of the method realloc, which for large chunks of memory uses mremap;
 * - passing the size into the `free` method;
 * - by the presence of the `alignment` argument;
 * - the possibility of zeroing memory (used in hash tables);
 * - random hint address for mmap
 * - mmap_threshold for using mmap less or more
 *
 * @see DB::IGrabberAllocator
 *
 * @tparam ClearMemory zero out memory (with memset) on alloc or realloc, if the size is less than MMAP_THRESHOLD (in
 *         other case the memory is mmaped, so zeroing is guaranteed).
 */
template <bool ClearMemory>
class Allocator
{
private:
    static constexpr auto ASLR = AllocatorsASLR();

public:
    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 0)
    {
        CurrentMemoryTracker::alloc(size);
        return allocNoTrack(size, alignment);
    }

    /// Free memory range.
    void free(void * buf, size_t size)
    {
        freeNoTrack(buf, size);
        CurrentMemoryTracker::free(size);
    }

    /**
     * @brief Enlarge memory range. Data from old range is moved to the beginning of new range.  Address of memory range
     *        could change.
     */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        if (old_size == new_size)
        {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        }
        else if (old_size < MMAP_THRESHOLD &&
                 new_size < MMAP_THRESHOLD &&
                 alignment <= MALLOC_MIN_ALIGNMENT)
        {
            /// Resize malloc'd memory region with no special alignment requirement.
            CurrentMemoryTracker::realloc(old_size, new_size);

            void * new_buf = ::realloc(buf, new_size);

            if (nullptr == new_buf)
                DB::throwFromErrno(
                        "Allocator: Cannot realloc from " + formatReadableSizeWithBinarySuffix(old_size) +
                        " to " + formatReadableSizeWithBinarySuffix(new_size) + ".",
                        DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

            buf = new_buf;

            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
        }
        else if (old_size >= MMAP_THRESHOLD &&
                 new_size >= MMAP_THRESHOLD)
        {
            /// Resize mmap'd memory region.
            CurrentMemoryTracker::realloc(old_size, new_size);

            // On apple and freebsd self-implemented mremap used (common/mremap.h)
            buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE,
                                    PROT_READ | PROT_WRITE,
                                    MAP_PRIVATE | MAP_ANONYMOUS | PossibleMmapPopulate,
                                    -1, 0);

            if (MAP_FAILED == buf)
                DB::throwFromErrno(
                        "Allocator: Cannot mremap memory chunk from " + formatReadableSizeWithBinarySuffix(old_size) +
                        " to " + formatReadableSizeWithBinarySuffix(new_size) + ".",
                        DB::ErrorCodes::CANNOT_MREMAP);

            /// No need for zero-filling, because mmap guarantees it.
        }
        else if (new_size < MMAP_THRESHOLD)
        {
            /// Small allocs that requires a copy. Assume there's enough memory in system. Call CurrentMemoryTracker
            /// once.
            CurrentMemoryTracker::realloc(old_size, new_size);

            void * new_buf = allocNoTrack(new_size, alignment);

            memcpy(new_buf, buf, std::min(old_size, new_size));

            freeNoTrack(buf, old_size);

            buf = new_buf;
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
    /**
     * @brief Various allocators' interfaces may allow you to allocate memory on the stack. 0 represents "memory is not
     *        allocated on the stack".
     */
    static constexpr size_t getStackThreshold() noexcept
    {
        return 0;
    }

    static constexpr bool clear_memory = ClearMemory;

private:
    void * allocNoTrack(size_t size, size_t alignment)
    {
        void * buf;

        if (size >= MMAP_THRESHOLD)
        {
            if (alignment > MMAP_MIN_ALIGNMENT)
                throw DB::Exception(
                        "Too large alignment " + formatReadableSizeWithBinarySuffix(alignment) +
                        ": more than page size when allocating " + formatReadableSizeWithBinarySuffix(size) + ".",
                        DB::ErrorCodes::BAD_ARGUMENTS);

            buf = mmap(ASLR(),
                    size,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | PossibleMmapPopulate,
                    -1,
                    0);

            if (MAP_FAILED == buf)
                DB::throwFromErrno(
                        "Allocator: Cannot mmap " + formatReadableSizeWithBinarySuffix(size) + ".",
                        DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

            /// No need for zero-fill, because mmap guarantees it.
        }
        else
        {
            if (alignment <= MALLOC_MIN_ALIGNMENT)
            {
                if constexpr (clear_memory)
                    buf = ::calloc(size, 1);
                else
                    buf = ::malloc(size);

                if (nullptr == buf)
                    DB::throwFromErrno(
                            "Allocator: Cannot malloc " + formatReadableSizeWithBinarySuffix(size) + ".",
                            DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
            }
            else
            {
                buf = nullptr;

                if (int res = posix_memalign(&buf, alignment, size); res)
                    DB::throwFromErrno(
                            "Cannot allocate memory (posix_memalign) " + formatReadableSizeWithBinarySuffix(size) + ".",
                            DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                            res);

                if constexpr (clear_memory)
                    memset(buf, 0, size);
            }
        }

        return buf;
    }

    void freeNoTrack(void * buf, size_t size)
    {
        if (size >= MMAP_THRESHOLD)
        {
            if (munmap(buf, size))
                DB::throwFromErrno(
                        "Allocator: Cannot munmap " + formatReadableSizeWithBinarySuffix(size) + ".",
                        DB::ErrorCodes::CANNOT_MUNMAP);
        }
        else
        {
            ::free(buf);
        }
    }

};

/** When using AllocatorWithStackMemory, located on the stack,
  *  GCC 4.9 mistakenly assumes that we can call `free` from a pointer to the stack.
  * In fact, the combination of conditions inside AllocatorWithStackMemory does not allow this.
  */
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif

/** Allocator with optimization to place small memory ranges in automatic memory.
  */
template <typename Base, size_t N, size_t Alignment>
class AllocatorWithStackMemory : private Base
{
private:
    alignas(Alignment) char stack_memory[N];

public:
    /// Do not use boost::noncopyable to avoid the warning about direct base
    /// being inaccessible due to ambiguity, when derived classes are also
    /// noncopiable (-Winaccessible-base).
    AllocatorWithStackMemory(const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory & operator = (const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory() = default;
    ~AllocatorWithStackMemory() = default;

    void * alloc(size_t size)
    {
        if (size <= N)
        {
            if constexpr (Base::clear_memory)
                memset(stack_memory, 0, N);
            return stack_memory;
        }

        return Base::alloc(size, Alignment);
    }

    void free(void * buf, size_t size)
    {
        if (size > N)
            Base::free(buf, size);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size)
    {
        /// Was in stack_memory, will remain there.
        if (new_size <= N)
            return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > N)
            return Base::realloc(buf, old_size, new_size, Alignment);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size, Alignment);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return N;
    }
};


#if !__clang__
#pragma GCC diagnostic pop
#endif
