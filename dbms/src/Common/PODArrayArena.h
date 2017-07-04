#include <Common/PODArray.h>
#include <Common/Arena.h>


namespace DB
{

/// Fake Allocator which proxies all allocations to Arena
/// Used in aggregate functions
struct ArenaAllocator
{
    void * alloc(size_t size, Arena * arena)
    {
        return arena->alloc(size);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        char const * data = reinterpret_cast<char *>(buf);

        if (data + old_size == arena->head->pos)
        {
            // Consecutive optimization
            // Invariant should be maintained: new_size > old_size

            arena->allocContinue(new_size - old_size, data);
            return reinterpret_cast<void *>(const_cast<char *>(data));
        }
        else
        {
            return arena->realloc(data, old_size, new_size);
        }
    }

    void free(void * buf, size_t size) {}
};


template <size_t N = 64, typename Base = ArenaAllocator>
class ArenaAllocatorWithStackMemoty : public Base
{
    char stack_memory[N];

public:

    void * alloc(size_t size, Arena * arena)
    {
        return (size > N) ? Base::alloc(size, arena) : stack_memory;
    }

    void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        /// Was in stack_memory, will remain there.
        if (new_size <= N)
            return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > N)
            return Base::realloc(buf, old_size, new_size, arena);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size, arena);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

    void free(void * buf, size_t size) {}
};


/// Similar to PODArray, but allocates memory using ArenaAllocator
template <typename T, size_t INITIAL_SIZE = 32, typename TAllocator = ArenaAllocator>
class PODArrayArenaAllocator : private TAllocator
{
    char * c_start          = nullptr;
    char * c_end            = nullptr;
    char * c_end_of_storage = nullptr;

    T * t_start()                      { return reinterpret_cast<T *>(c_start); }
    T * t_end()                        { return reinterpret_cast<T *>(c_end); }
    T * t_end_of_storage()             { return reinterpret_cast<T *>(c_end_of_storage); }

    const T * t_start() const          { return reinterpret_cast<const T *>(c_start); }
    const T * t_end() const            { return reinterpret_cast<const T *>(c_end); }
    const T * t_end_of_storage() const { return reinterpret_cast<const T *>(c_end_of_storage); }

    /// The amount of memory occupied by the num_elements of the elements.
    static size_t byte_size(size_t num_elements) { return num_elements * sizeof(T); }

    /// Minimum amount of memory to allocate for num_elements, including padding.
    static size_t minimum_memory_for_elements(size_t num_elements) { return byte_size(num_elements); }

    void alloc(size_t bytes, Arena * arena)
    {
        c_start = c_end = reinterpret_cast<char *>(TAllocator::alloc(bytes, arena));
        c_end_of_storage = c_start + bytes;
    }

    void realloc(size_t bytes, Arena * arena)
    {
        if (c_start == nullptr)
        {
            alloc(bytes, arena);
            return;
        }

        ptrdiff_t end_diff = c_end - c_start;

        c_start = reinterpret_cast<char *>(TAllocator::realloc(c_start, allocated_size(), bytes, arena));

        c_end = c_start + end_diff;
        c_end_of_storage = c_start + bytes;
    }

    bool isInitialized() const
    {
        return (c_start != nullptr) && (c_end != nullptr) && (c_end_of_storage != nullptr);
    }

public:

    size_t allocated_size() const { return c_end_of_storage - c_start; }

    T & operator[] (size_t n)                 { return t_start()[n]; }
    const T & operator[] (size_t n) const     { return t_start()[n]; }

    size_t size() const { return t_end() - t_start(); }
    bool empty() const { return t_end() == t_start(); }
    size_t capacity() const { return t_end_of_storage() - t_start(); }


    /// You can not just use `typedef`, because there is ambiguity for the constructors and `assign` functions.
    struct iterator : public boost::iterator_adaptor<iterator, T*>
    {
        iterator() {}
        iterator(T * ptr_) : iterator::iterator_adaptor_(ptr_) {}
    };

    struct const_iterator : public boost::iterator_adaptor<const_iterator, const T*>
    {
        const_iterator() {}
        const_iterator(const T * ptr_) : const_iterator::iterator_adaptor_(ptr_) {}
    };

    iterator begin()              { return t_start(); }
    iterator end()                { return t_end(); }
    const_iterator begin() const  { return t_start(); }
    const_iterator end() const    { return t_end(); }
    const_iterator cbegin() const { return t_start(); }
    const_iterator cend() const   { return t_end(); }

    void resize(size_t n, Arena * arena)
    {
        reserve(n, arena);
        resize_assume_reserved(n);
    }

    void resize_assume_reserved(const size_t n)
    {
        c_end = c_start + byte_size(n);
    }

    template <typename It1, typename It2>
    void insert_assume_reserved(It1 from_begin, It2 from_end)
    {
        size_t bytes_to_copy = byte_size(from_end - from_begin);
        memcpy(c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
        c_end += bytes_to_copy;
    }

    void reserve(size_t n, Arena * arena)
    {
        if (n > capacity())
            realloc(roundUpToPowerOfTwoOrZero(minimum_memory_for_elements(n)), arena);
    }

    void reserve(Arena * arena)
    {
        if (size() == 0)
            realloc(std::max(INITIAL_SIZE, minimum_memory_for_elements(1)), arena);
        else
            realloc(allocated_size() * 2, arena);
    }

    void push_back(const T & x, Arena * arena)
    {
        if (unlikely(c_end == c_end_of_storage))
            reserve(arena);

        *t_end() = x;
        c_end += byte_size(1);
    }

    template <typename It1, typename It2>
    void insert(It1 from_begin, It2 from_end, Arena * arena)
    {
        size_t required_capacity = size() + (from_end - from_begin);
        if (required_capacity > capacity())
            reserve(roundUpToPowerOfTwoOrZero(required_capacity), arena);

        insert_assume_reserved(from_begin, from_end);
    }
};


}
