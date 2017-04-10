#pragma once

#include <string.h>


/** Отвечает за выделение/освобождение памяти. Используется, например, в PODArray, Arena.
  * Также используется в хэш-таблицах.
  * Интерфейс отличается от std::allocator
  * - наличием метода realloc, который для больших кусков памяти использует mremap;
  * - передачей размера в метод free;
  * - наличием аргумента alignment;
  * - возможностью зануления памяти (используется в хэш-таблицах);
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


/** При использовании AllocatorWithStackMemory, размещённом на стеке,
  *  GCC 4.9 ошибочно делает предположение, что мы можем вызывать free от указателя на стек.
  * На самом деле, комбинация условий внутри AllocatorWithStackMemory этого не допускает.
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
