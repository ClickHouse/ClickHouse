#pragma once

#include <string.h>
#include <cstddef>
#include <cassert>
#include <algorithm>
#include <memory>

#include <boost/noncopyable.hpp>

#include <common/strong_typedef.h>

#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Common/BitHelpers.h>
#include <Common/memcpySmall.h>

#ifndef NDEBUG
    #include <sys/mman.h>
#endif

#include <Common/PODArray_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MPROTECT;
}

/** A dynamic array for POD types.
  * Designed for a small number of large arrays (rather than a lot of small ones).
  * To be more precise - for use in ColumnVector.
  * It differs from std::vector in that it does not initialize the elements.
  *
  * Made noncopyable so that there are no accidential copies. You can copy the data using `assign` method.
  *
  * Only part of the std::vector interface is supported.
  *
  * The default constructor creates an empty object that does not allocate memory.
  * Then the memory is allocated at least initial_bytes bytes.
  *
  * If you insert elements with push_back, without making a `reserve`, then PODArray is about 2.5 times faster than std::vector.
  *
  * The template parameter `pad_right` - always allocate at the end of the array as many unused bytes.
  * Can be used to make optimistic reading, writing, copying with unaligned SIMD instructions.
  *
  * The template parameter `pad_left` - always allocate memory before 0th element of the array (rounded up to the whole number of elements)
  *  and zero initialize -1th element. It allows to use -1th element that will have value 0.
  * This gives performance benefits when converting an array of offsets to array of sizes.
  *
  * Some methods using allocator have TAllocatorParams variadic arguments.
  * These arguments will be passed to corresponding methods of TAllocator.
  * Example: pointer to Arena, that is used for allocations.
  *
  * Why Allocator is not passed through constructor, as it is done in C++ standard library?
  * Because sometimes we have many small objects, that share same allocator with same parameters,
  *  and we must avoid larger object size due to storing the same parameters in each object.
  * This is required for states of aggregate functions.
  *
  * TODO Pass alignment to Allocator.
  * TODO Allow greater alignment than alignof(T). Example: array of char aligned to page size.
  */
static constexpr size_t empty_pod_array_size = 1024;
extern const char empty_pod_array[empty_pod_array_size];

/** Base class that depend only on size of element, not on element itself.
  * You can static_cast to this class if you want to insert some data regardless to the actual type T.
  */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnull-dereference"

template <size_t ELEMENT_SIZE, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_>
class PODArrayBase : private boost::noncopyable, private TAllocator    /// empty base optimization
{
protected:
    /// Round padding up to an whole number of elements to simplify arithmetic.
    static constexpr size_t pad_right = integerRoundUp(pad_right_, ELEMENT_SIZE);
    /// pad_left is also rounded up to 16 bytes to maintain alignment of allocated memory.
    static constexpr size_t pad_left = integerRoundUp(integerRoundUp(pad_left_, ELEMENT_SIZE), 16);
    /// Empty array will point to this static memory as padding.
    static constexpr char * null = pad_left ? const_cast<char *>(empty_pod_array) + empty_pod_array_size : nullptr;

    static_assert(pad_left <= empty_pod_array_size && "Left Padding exceeds empty_pod_array_size. Is the element size too large?");

    // If we are using allocator with inline memory, the minimal size of
    // array must be in sync with the size of this memory.
    static_assert(allocatorInitialBytes<TAllocator> == 0
                  || allocatorInitialBytes<TAllocator> == initial_bytes);

    char * c_start          = null;    /// Does not include pad_left.
    char * c_end            = null;
    char * c_end_of_storage = null;    /// Does not include pad_right.

    /// The amount of memory occupied by the num_elements of the elements.
    static size_t byte_size(size_t num_elements) { return num_elements * ELEMENT_SIZE; }

    /// Minimum amount of memory to allocate for num_elements, including padding.
    static size_t minimum_memory_for_elements(size_t num_elements) { return byte_size(num_elements) + pad_right + pad_left; }

    void alloc_for_num_elements(size_t num_elements)
    {
        alloc(roundUpToPowerOfTwoOrZero(minimum_memory_for_elements(num_elements)));
    }

    template <typename ... TAllocatorParams>
    void alloc(size_t bytes, TAllocatorParams &&... allocator_params)
    {
        c_start = c_end = reinterpret_cast<char *>(TAllocator::alloc(bytes, std::forward<TAllocatorParams>(allocator_params)...)) + pad_left;
        c_end_of_storage = c_start + bytes - pad_right - pad_left;

        if (pad_left)
            memset(c_start - ELEMENT_SIZE, 0, ELEMENT_SIZE);
    }

    void dealloc()
    {
        if (c_start == null)
            return;

        unprotect();

        TAllocator::free(c_start - pad_left, allocated_bytes());
    }

    template <typename ... TAllocatorParams>
    void realloc(size_t bytes, TAllocatorParams &&... allocator_params)
    {
        if (c_start == null)
        {
            alloc(bytes, std::forward<TAllocatorParams>(allocator_params)...);
            return;
        }

        unprotect();

        ptrdiff_t end_diff = c_end - c_start;

        c_start = reinterpret_cast<char *>(
                TAllocator::realloc(c_start - pad_left, allocated_bytes(), bytes, std::forward<TAllocatorParams>(allocator_params)...))
            + pad_left;

        c_end = c_start + end_diff;
        c_end_of_storage = c_start + bytes - pad_right - pad_left;
    }

    bool isInitialized() const
    {
        return (c_start != null) && (c_end != null) && (c_end_of_storage != null);
    }

    bool isAllocatedFromStack() const
    {
        static constexpr size_t stack_threshold = TAllocator::getStackThreshold();
        return (stack_threshold > 0) && (allocated_bytes() <= stack_threshold);
    }

    template <typename ... TAllocatorParams>
    void reserveForNextSize(TAllocatorParams &&... allocator_params)
    {
        if (empty())
        {
            // The allocated memory should be multiplication of ELEMENT_SIZE to hold the element, otherwise,
            // memory issue such as corruption could appear in edge case.
            realloc(std::max(integerRoundUp(initial_bytes, ELEMENT_SIZE),
                             minimum_memory_for_elements(1)),
                    std::forward<TAllocatorParams>(allocator_params)...);
        }
        else
            realloc(allocated_bytes() * 2, std::forward<TAllocatorParams>(allocator_params)...);
    }

#ifndef NDEBUG
    /// Make memory region readonly with mprotect if it is large enough.
    /// The operation is slow and performed only for debug builds.
    void protectImpl(int prot)
    {
        static constexpr size_t PROTECT_PAGE_SIZE = 4096;

        char * left_rounded_up = reinterpret_cast<char *>((reinterpret_cast<intptr_t>(c_start) - pad_left + PROTECT_PAGE_SIZE - 1) / PROTECT_PAGE_SIZE * PROTECT_PAGE_SIZE);
        char * right_rounded_down = reinterpret_cast<char *>((reinterpret_cast<intptr_t>(c_end_of_storage) + pad_right) / PROTECT_PAGE_SIZE * PROTECT_PAGE_SIZE);

        if (right_rounded_down > left_rounded_up)
        {
            size_t length = right_rounded_down - left_rounded_up;
            if (0 != mprotect(left_rounded_up, length, prot))
                throwFromErrno("Cannot mprotect memory region", ErrorCodes::CANNOT_MPROTECT);
        }
    }

    /// Restore memory protection in destructor or realloc for further reuse by allocator.
    bool mprotected = false;
#endif

public:
    bool empty() const { return c_end == c_start; }
    size_t size() const { return (c_end - c_start) / ELEMENT_SIZE; }
    size_t capacity() const { return (c_end_of_storage - c_start) / ELEMENT_SIZE; }

    /// This method is safe to use only for information about memory usage.
    size_t allocated_bytes() const { return c_end_of_storage - c_start + pad_right + pad_left; }

    void clear() { c_end = c_start; }

    template <typename ... TAllocatorParams>
    void reserve(size_t n, TAllocatorParams &&... allocator_params)
    {
        if (n > capacity())
            realloc(roundUpToPowerOfTwoOrZero(minimum_memory_for_elements(n)), std::forward<TAllocatorParams>(allocator_params)...);
    }

    template <typename ... TAllocatorParams>
    void resize(size_t n, TAllocatorParams &&... allocator_params)
    {
        reserve(n, std::forward<TAllocatorParams>(allocator_params)...);
        resize_assume_reserved(n);
    }

    void resize_assume_reserved(const size_t n)
    {
        c_end = c_start + byte_size(n);
    }

    const char * raw_data() const
    {
        return c_start;
    }

    template <typename ... TAllocatorParams>
    void push_back_raw(const void * ptr, TAllocatorParams &&... allocator_params)
    {
        push_back_raw_many(1, ptr, std::forward<TAllocatorParams>(allocator_params)...);
    }

    template <typename ... TAllocatorParams>
    void push_back_raw_many(size_t number_of_items, const void * ptr, TAllocatorParams &&... allocator_params)
    {
        if (unlikely(c_end == c_end_of_storage))
            reserve(number_of_items, std::forward<TAllocatorParams>(allocator_params)...);

        memcpy(c_end, ptr, ELEMENT_SIZE * number_of_items);
        c_end += byte_size(number_of_items);
    }

    void protect()
    {
#ifndef NDEBUG
        protectImpl(PROT_READ);
        mprotected = true;
#endif
    }

    void unprotect()
    {
#ifndef NDEBUG
        if (mprotected)
            protectImpl(PROT_WRITE);
        mprotected = false;
#endif
    }

    ~PODArrayBase()
    {
        dealloc();
    }
};

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_>
class PODArray : public PODArrayBase<sizeof(T), initial_bytes, TAllocator, pad_right_, pad_left_>
{
protected:
    using Base = PODArrayBase<sizeof(T), initial_bytes, TAllocator, pad_right_, pad_left_>;

    T * t_start()                      { return reinterpret_cast<T *>(this->c_start); }
    T * t_end()                        { return reinterpret_cast<T *>(this->c_end); }
    T * t_end_of_storage()             { return reinterpret_cast<T *>(this->c_end_of_storage); }

    const T * t_start() const          { return reinterpret_cast<const T *>(this->c_start); }
    const T * t_end() const            { return reinterpret_cast<const T *>(this->c_end); }
    const T * t_end_of_storage() const { return reinterpret_cast<const T *>(this->c_end_of_storage); }

public:
    using value_type = T;

    /// We cannot use boost::iterator_adaptor, because it defeats loop vectorization,
    ///  see https://github.com/ClickHouse/ClickHouse/pull/9442

    using iterator = T *;
    using const_iterator = const T *;


    PODArray() {}

    PODArray(size_t n)
    {
        this->alloc_for_num_elements(n);
        this->c_end += this->byte_size(n);
    }

    PODArray(size_t n, const T & x)
    {
        this->alloc_for_num_elements(n);
        assign(n, x);
    }

    PODArray(const_iterator from_begin, const_iterator from_end)
    {
        this->alloc_for_num_elements(from_end - from_begin);
        insert(from_begin, from_end);
    }

    PODArray(std::initializer_list<T> il) : PODArray(std::begin(il), std::end(il)) {}

    PODArray(PODArray && other)
    {
        this->swap(other);
    }

    PODArray & operator=(PODArray && other)
    {
        this->swap(other);
        return *this;
    }

    T * data() { return t_start(); }
    const T * data() const { return t_start(); }

    /// The index is signed to access -1th element without pointer overflow.
    T & operator[] (ssize_t n)
    {
        /// <= size, because taking address of one element past memory range is Ok in C++ (expression like &arr[arr.size()] is perfectly valid).
        assert((n >= (static_cast<ssize_t>(pad_left_) ? -1 : 0)) && (n <= static_cast<ssize_t>(this->size())));
        return t_start()[n];
    }

    const T & operator[] (ssize_t n) const
    {
        assert((n >= (static_cast<ssize_t>(pad_left_) ? -1 : 0)) && (n <= static_cast<ssize_t>(this->size())));
        return t_start()[n];
    }

    T & front()             { return t_start()[0]; }
    T & back()              { return t_end()[-1]; }
    const T & front() const { return t_start()[0]; }
    const T & back() const  { return t_end()[-1]; }

    iterator begin()              { return t_start(); }
    iterator end()                { return t_end(); }
    const_iterator begin() const  { return t_start(); }
    const_iterator end() const    { return t_end(); }
    const_iterator cbegin() const { return t_start(); }
    const_iterator cend() const   { return t_end(); }

    /// Same as resize, but zeroes new elements.
    void resize_fill(size_t n)
    {
        size_t old_size = this->size();
        if (n > old_size)
        {
            this->reserve(n);
            memset(this->c_end, 0, this->byte_size(n - old_size));
        }
        this->c_end = this->c_start + this->byte_size(n);
    }

    void resize_fill(size_t n, const T & value)
    {
        size_t old_size = this->size();
        if (n > old_size)
        {
            this->reserve(n);
            std::fill(t_end(), t_end() + n - old_size, value);
        }
        this->c_end = this->c_start + this->byte_size(n);
    }

    template <typename U, typename ... TAllocatorParams>
    void push_back(U && x, TAllocatorParams &&... allocator_params)
    {
        if (unlikely(this->c_end == this->c_end_of_storage))
            this->reserveForNextSize(std::forward<TAllocatorParams>(allocator_params)...);

        new (t_end()) T(std::forward<U>(x));
        this->c_end += this->byte_size(1);
    }

    /** This method doesn't allow to pass parameters for Allocator,
      *  and it couldn't be used if Allocator requires custom parameters.
      */
    template <typename... Args>
    void emplace_back(Args &&... args)
    {
        if (unlikely(this->c_end == this->c_end_of_storage))
            this->reserveForNextSize();

        new (t_end()) T(std::forward<Args>(args)...);
        this->c_end += this->byte_size(1);
    }

    void pop_back()
    {
        this->c_end -= this->byte_size(1);
    }

    /// Do not insert into the array a piece of itself. Because with the resize, the iterators on themselves can be invalidated.
    template <typename It1, typename It2, typename ... TAllocatorParams>
    void insertPrepare(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        size_t required_capacity = this->size() + (from_end - from_begin);
        if (required_capacity > this->capacity())
            this->reserve(roundUpToPowerOfTwoOrZero(required_capacity), std::forward<TAllocatorParams>(allocator_params)...);
    }

    /// Do not insert into the array a piece of itself. Because with the resize, the iterators on themselves can be invalidated.
    template <typename It1, typename It2, typename ... TAllocatorParams>
    void insert(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        insertPrepare(from_begin, from_end, std::forward<TAllocatorParams>(allocator_params)...);
        insert_assume_reserved(from_begin, from_end);
    }

    /// Works under assumption, that it's possible to read up to 15 excessive bytes after `from_end` and this PODArray is padded.
    template <typename It1, typename It2, typename ... TAllocatorParams>
    void insertSmallAllowReadWriteOverflow15(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        static_assert(pad_right_ >= 15);
        insertPrepare(from_begin, from_end, std::forward<TAllocatorParams>(allocator_params)...);
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        memcpySmallAllowReadWriteOverflow15(this->c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    template <typename It1, typename It2>
    void insert(iterator it, It1 from_begin, It2 from_end)
    {
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        size_t bytes_to_move = (end() - it) * sizeof(T);

        insertPrepare(from_begin, from_end);

        if (unlikely(bytes_to_move))
            memcpy(this->c_end + bytes_to_copy - bytes_to_move, this->c_end - bytes_to_move, bytes_to_move);

        memcpy(this->c_end - bytes_to_move, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    template <typename It1, typename It2>
    void insert_assume_reserved(It1 from_begin, It2 from_end)
    {
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        memcpy(this->c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    template <typename... TAllocatorParams>
    void swap(PODArray & rhs, TAllocatorParams &&... allocator_params)
    {
#ifndef NDEBUG
        this->unprotect();
        rhs.unprotect();
#endif

        /// Swap two PODArray objects, arr1 and arr2, that satisfy the following conditions:
        /// - The elements of arr1 are stored on stack.
        /// - The elements of arr2 are stored on heap.
        auto swap_stack_heap = [&](PODArray & arr1, PODArray & arr2)
        {
            size_t stack_size = arr1.size();
            size_t stack_allocated = arr1.allocated_bytes();

            size_t heap_size = arr2.size();
            size_t heap_allocated = arr2.allocated_bytes();

            /// Keep track of the stack content we have to copy.
            char * stack_c_start = arr1.c_start;

            /// arr1 takes ownership of the heap memory of arr2.
            arr1.c_start = arr2.c_start;
            arr1.c_end_of_storage = arr1.c_start + heap_allocated - arr1.pad_right;
            arr1.c_end = arr1.c_start + this->byte_size(heap_size);

            /// Allocate stack space for arr2.
            arr2.alloc(stack_allocated, std::forward<TAllocatorParams>(allocator_params)...);
            /// Copy the stack content.
            memcpy(arr2.c_start, stack_c_start, this->byte_size(stack_size));
            arr2.c_end = arr2.c_start + this->byte_size(stack_size);
        };

        auto do_move = [&](PODArray & src, PODArray & dest)
        {
            if (src.isAllocatedFromStack())
            {
                dest.dealloc();
                dest.alloc(src.allocated_bytes(), std::forward<TAllocatorParams>(allocator_params)...);
                memcpy(dest.c_start, src.c_start, this->byte_size(src.size()));
                dest.c_end = dest.c_start + (src.c_end - src.c_start);

                src.c_start = Base::null;
                src.c_end = Base::null;
                src.c_end_of_storage = Base::null;
            }
            else
            {
                std::swap(dest.c_start, src.c_start);
                std::swap(dest.c_end, src.c_end);
                std::swap(dest.c_end_of_storage, src.c_end_of_storage);
            }
        };

        if (!this->isInitialized() && !rhs.isInitialized())
        {
            return;
        }
        else if (!this->isInitialized() && rhs.isInitialized())
        {
            do_move(rhs, *this);
            return;
        }
        else if (this->isInitialized() && !rhs.isInitialized())
        {
            do_move(*this, rhs);
            return;
        }

        if (this->isAllocatedFromStack() && rhs.isAllocatedFromStack())
        {
            size_t min_size = std::min(this->size(), rhs.size());
            size_t max_size = std::max(this->size(), rhs.size());

            for (size_t i = 0; i < min_size; ++i)
                std::swap(this->operator[](i), rhs[i]);

            if (this->size() == max_size)
            {
                for (size_t i = min_size; i < max_size; ++i)
                    rhs[i] = this->operator[](i);
            }
            else
            {
                for (size_t i = min_size; i < max_size; ++i)
                    this->operator[](i) = rhs[i];
            }

            size_t lhs_size = this->size();
            size_t lhs_allocated = this->allocated_bytes();

            size_t rhs_size = rhs.size();
            size_t rhs_allocated = rhs.allocated_bytes();

            this->c_end_of_storage = this->c_start + rhs_allocated - Base::pad_right;
            rhs.c_end_of_storage = rhs.c_start + lhs_allocated - Base::pad_right;

            this->c_end = this->c_start + this->byte_size(rhs_size);
            rhs.c_end = rhs.c_start + this->byte_size(lhs_size);
        }
        else if (this->isAllocatedFromStack() && !rhs.isAllocatedFromStack())
        {
            swap_stack_heap(*this, rhs);
        }
        else if (!this->isAllocatedFromStack() && rhs.isAllocatedFromStack())
        {
            swap_stack_heap(rhs, *this);
        }
        else
        {
            std::swap(this->c_start, rhs.c_start);
            std::swap(this->c_end, rhs.c_end);
            std::swap(this->c_end_of_storage, rhs.c_end_of_storage);
        }
    }

    template <typename... TAllocatorParams>
    void assign(size_t n, const T & x, TAllocatorParams &&... allocator_params)
    {
        this->resize(n, std::forward<TAllocatorParams>(allocator_params)...);
        std::fill(begin(), end(), x);
    }

    template <typename It1, typename It2, typename... TAllocatorParams>
    void assign(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        size_t required_capacity = from_end - from_begin;
        if (required_capacity > this->capacity())
            this->reserve(roundUpToPowerOfTwoOrZero(required_capacity), std::forward<TAllocatorParams>(allocator_params)...);

        size_t bytes_to_copy = this->byte_size(required_capacity);
        memcpy(this->c_start, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
        this->c_end = this->c_start + bytes_to_copy;
    }

    // ISO C++ has strict ambiguity rules, thus we cannot apply TAllocatorParams here.
    void assign(const PODArray & from)
    {
        assign(from.begin(), from.end());
    }


    bool operator== (const PODArray & other) const
    {
        if (this->size() != other.size())
            return false;

        const_iterator this_it = begin();
        const_iterator that_it = other.begin();

        while (this_it != end())
        {
            if (*this_it != *that_it)
                return false;

            ++this_it;
            ++that_it;
        }

        return true;
    }

    bool operator!= (const PODArray & other) const
    {
        return !operator==(other);
    }
};

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_>
void swap(PODArray<T, initial_bytes, TAllocator, pad_right_> & lhs, PODArray<T, initial_bytes, TAllocator, pad_right_> & rhs)
{
    lhs.swap(rhs);
}
#pragma GCC diagnostic pop

}
