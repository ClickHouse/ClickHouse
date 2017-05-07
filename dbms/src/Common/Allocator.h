#pragma once

#include <string.h>


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
protected:
    static constexpr bool clear_memory = clear_memory_;

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
template <typename Base, size_t N = 64>
class AllocatorWithStackMemory : private Base
{
private:
    char stack_memory[N];

public:
    void * alloc(size_t size)
    {
        if (size <= N)
        {
            if (Base::clear_memory)
                memset(stack_memory, 0, N);
            return stack_memory;
        }

        return Base::alloc(size);
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
            return Base::realloc(buf, old_size, new_size);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size);
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
