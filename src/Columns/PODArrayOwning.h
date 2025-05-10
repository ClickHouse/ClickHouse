#pragma once

#include <memory>
#include "IBuffer.h"

namespace DB
{

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_> // NOLINT
class PODArrayOwning : public IBuffer<T, initial_bytes, TAllocator, pad_right_, pad_left_>, public std::enable_shared_from_this<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>>
{
protected:
    using Base = IBuffer<T, initial_bytes, TAllocator, pad_right_, pad_left_>;
    static constexpr size_t ELEMENT_SIZE = sizeof(T);
public:
    using value_type = T;
    using iterator = T *;
    using const_iterator = const T *;

    PODArrayOwning() = default;

    explicit PODArrayOwning(size_t n)
    {
        this->alloc_for_num_elements(n);
        this->c_end += BufferDetails::byte_size(n, sizeof(T));
    }

    PODArrayOwning(size_t n, const T & x)
    {
        this->alloc_for_num_elements(n);
        assign(n, x);
    }

    PODArrayOwning(const_iterator from_begin, const_iterator from_end)
    {
        this->alloc_for_num_elements(from_end - from_begin);
        insert(from_begin, from_end);
    }

    PODArrayOwning(std::initializer_list<T> il)
    {
        this->reserve(std::size(il));

        for (const auto & x : il)
        {
            this->push_back(x);
        }
    }

    PODArrayOwning(PODArrayOwning && other) noexcept
    {
        this->swap(other);
    }

    PODArrayOwning & operator=(PODArrayOwning && other) noexcept
    {
        this->swap(other);
        return *this;
    }

    std::shared_ptr<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>> getOwningBuffer() final {
        return this->shared_from_this();
    }

    void alloc_for_num_elements(size_t num_elements) override
    {
        alloc(PODArrayDetails::minimum_memory_for_elements(num_elements, sizeof(T), Base::pad_left, Base::pad_right));
    }

    void alloc(size_t bytes) override
    {
        char * allocated = reinterpret_cast<char *>(TAllocator::alloc(bytes));

        this->c_start = allocated + Base::pad_left;
        this->c_end = this->c_start;
        this->c_end_of_storage = allocated + bytes - Base::pad_right;

        if (Base::pad_left)
            memset(this->c_start - ELEMENT_SIZE, 0, ELEMENT_SIZE);
    }

    void dealloc() override
    {
        if (this->c_start == Base::null)
            return;

        // unprotect();

        TAllocator::free(this->c_start - Base::pad_left, this->allocated_bytes());
    }

    void realloc(size_t bytes) override
    {
        if (this->c_start == Base::null)
        {
            alloc(bytes);
            return;
        }

        // unprotect();

        ptrdiff_t end_diff = this->c_end - this->c_start;

        char * allocated = reinterpret_cast<char *>(
            TAllocator::realloc(this->c_start - Base::pad_left, this->allocated_bytes(), bytes));

        this->c_start = allocated + Base::pad_left;
        this->c_end = this->c_start + end_diff;
        this->c_end_of_storage = allocated + bytes - Base::pad_right;
    }

    template <typename ... TAllocatorParams>
    ALWAYS_INLINE /// Better performance in clang build, worse performance in gcc build.
    void reserve(size_t n, TAllocatorParams &&... allocator_params)
    {
        if (n > Base::capacity())
            realloc(roundUpToPowerOfTwoOrZero(BufferDetails::minimum_memory_for_elements(n, Base::element_size, Base::pad_left, Base::pad_right)), std::forward<TAllocatorParams>(allocator_params)...);
    }

    template <typename ... TAllocatorParams>
    void reserve_exact(size_t n, TAllocatorParams &&... allocator_params) /// NOLINT
    {
        if (n > Base::capacity())
            realloc(BufferDetails::minimum_memory_for_elements(n, Base::element_size, Base::pad_left, Base::pad_right), std::forward<TAllocatorParams>(allocator_params)...);
    }

    template <typename ... TAllocatorParams>
    void resize(size_t n, TAllocatorParams &&... allocator_params)
    {
        reserve(n, std::forward<TAllocatorParams>(allocator_params)...);
        Base::resize_assume_reserved(n);
    }

    template <typename ... TAllocatorParams>
    void resize_exact(size_t n, TAllocatorParams &&... allocator_params) /// NOLINT
    {
        reserve_exact(n, std::forward<TAllocatorParams>(allocator_params)...);
        Base::resize_assume_reserved(n);
    }

    /// Same as resize, but zeroes new elements.
    void resize_fill(size_t n) /// NOLINT
    {
        size_t old_size = this->size();
        if (n > old_size)
        {
            this->reserve(n);
            memset(this->c_end, 0, BufferDetails::byte_size(n - old_size, sizeof(T)));
        }
        this->c_end = this->c_start + BufferDetails::byte_size(n, sizeof(T));
    }

    void resize_fill(size_t n, const T & value) /// NOLINT
    {
        size_t old_size = this->size();
        if (n > old_size)
        {
            this->reserve(n);
            std::fill(Base::t_end(), Base::t_end() + n - old_size, value);
        }
        this->c_end = this->c_start + BufferDetails::byte_size(n, sizeof(T));
    }

    template <typename ... TAllocatorParams>
    void shrink_to_fit(TAllocatorParams &&... allocator_params) /// NOLINT
    {
        realloc(BufferDetails::minimum_memory_for_elements(Base::size(), Base::element_size, Base::pad_left, Base::pad_right), std::forward<TAllocatorParams>(allocator_params)...);
    }

    template <typename U, typename ... TAllocatorParams>
    void push_back(U && x, TAllocatorParams &&... allocator_params) /// NOLINT
    {
        if (unlikely(this->c_end + sizeof(T) > this->c_end_of_storage))
            this->reserveForNextSize(std::forward<TAllocatorParams>(allocator_params)...);

        this->t_end();
        new (reinterpret_cast<void*>(Base::t_end())) T(std::forward<U>(x));
        this->c_end += sizeof(T);
    }

    template <typename ... TAllocatorParams>
    void push_back_raw(const void * ptr, TAllocatorParams &&... allocator_params) /// NOLINT
    {
        size_t required_capacity = Base::size() + Base::element_size;
        if (unlikely(required_capacity > Base::capacity()))
            reserve(required_capacity, std::forward<TAllocatorParams>(allocator_params)...);

        memcpy(Base::c_end, ptr, Base::element_size);
        Base::c_end += Base::element_size;
    }

    template <typename... Args>
    void emplace_back(Args &&... args) /// NOLINT
    {
        if (unlikely(this->c_end + sizeof(T) > this->c_end_of_storage))
            this->reserveForNextSize();

        new (Base::t_end()) T(std::forward<Args>(args)...);
        this->c_end += sizeof(T);
    }

    void pop_back() /// NOLINT
    {
        this->c_end -= sizeof(T);
    }

    /// Do not insert into the array a piece of itself. Because with the resize, the iterators on themselves can be invalidated.
    template <typename It1, typename It2, typename ... TAllocatorParams>
    void insertPrepare(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        this->assertNotIntersects(from_begin, from_end);
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

    /// In contrast to 'insert' this method is Ok even for inserting from itself.
    /// Because we obtain iterators after reserving memory.
    template <typename Container, typename ... TAllocatorParams>
    void insertByOffsets(Container && rhs, size_t from_begin, size_t from_end, TAllocatorParams &&... allocator_params)
    {
        static_assert(memcpy_can_be_used_for_assignment<std::decay_t<T>, std::decay_t<decltype(rhs.front())>>);

        assert(from_end >= from_begin);
        assert(from_end <= rhs.size());

        size_t required_capacity = this->size() + (from_end - from_begin);
        if (required_capacity > this->capacity())
            this->reserve(roundUpToPowerOfTwoOrZero(required_capacity), std::forward<TAllocatorParams>(allocator_params)...);

        size_t bytes_to_copy = BufferDetails::byte_size(from_end - from_begin, sizeof(T));
        if (bytes_to_copy)
        {
            memcpy(this->c_end, reinterpret_cast<const void *>(rhs.begin() + from_begin), bytes_to_copy);
            this->c_end += bytes_to_copy;
        }
    }

    /// Works under assumption, that it's possible to read up to 15 excessive bytes after `from_end` and this PODArray is padded.
    template <typename It1, typename It2, typename ... TAllocatorParams>
    void insertSmallAllowReadWriteOverflow15(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        static_assert(Base::pad_right >= PADDING_FOR_SIMD - 1);
        static_assert(sizeof(T) == sizeof(*from_begin));
        insertPrepare(from_begin, from_end, std::forward<TAllocatorParams>(allocator_params)...);
        size_t bytes_to_copy = BufferDetails::byte_size(from_end - from_begin, sizeof(T));
        memcpySmallAllowReadWriteOverflow15(this->c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    /// Do not insert into the array a piece of itself. Because with the resize, the iterators on themselves can be invalidated.
    template <typename It1, typename It2>
    void insert(iterator it, It1 from_begin, It2 from_end)
    {
        static_assert(memcpy_can_be_used_for_assignment<std::decay_t<T>, std::decay_t<decltype(*from_begin)>>);

        size_t bytes_to_copy = BufferDetails::byte_size(from_end - from_begin, sizeof(T));
        if (!bytes_to_copy)
            return;

        size_t bytes_to_move = BufferDetails::byte_size(Base::end() - it, sizeof(T));

        insertPrepare(from_begin, from_end);

        if (unlikely(bytes_to_move))
            memmove(this->c_end + bytes_to_copy - bytes_to_move, this->c_end - bytes_to_move, bytes_to_move);

        memcpy(this->c_end - bytes_to_move, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);

        this->c_end += bytes_to_copy;
    }

    template <typename ... TAllocatorParams>
    void insertFromItself(iterator from_begin, iterator from_end, TAllocatorParams && ... allocator_params)
    {
        static_assert(memcpy_can_be_used_for_assignment<std::decay_t<T>, std::decay_t<decltype(*from_begin)>>);

        /// Convert iterators to indexes because reserve can invalidate iterators
        size_t start_index = from_begin - Base::begin();
        size_t end_index = from_end - Base::begin();
        size_t copy_size = end_index - start_index;

        assert(start_index <= end_index);

        size_t required_capacity = this->size() + copy_size;
        if (required_capacity > this->capacity())
            this->reserve(roundUpToPowerOfTwoOrZero(required_capacity), std::forward<TAllocatorParams>(allocator_params)...);

        size_t bytes_to_copy = BufferDetails::byte_size(copy_size, sizeof(T));
        if (bytes_to_copy)
        {
            auto begin = this->c_start + BufferDetails::byte_size(start_index, sizeof(T));
            memcpy(this->c_end, reinterpret_cast<const void *>(&*begin), bytes_to_copy);
            this->c_end += bytes_to_copy;
        }
    }

    template <typename It1, typename It2>
    void insert_assume_reserved(It1 from_begin, It2 from_end) /// NOLINT
    {
        static_assert(memcpy_can_be_used_for_assignment<std::decay_t<T>, std::decay_t<decltype(*from_begin)>>);
        this->assertNotIntersects(from_begin, from_end);

        size_t bytes_to_copy = BufferDetails::byte_size(from_end - from_begin, sizeof(T));
        if (bytes_to_copy)
        {
            memcpy(this->c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
            this->c_end += bytes_to_copy;
        }
    }

    template <typename... TAllocatorParams>
    void swap(PODArrayOwning& rhs, TAllocatorParams &&... allocator_params) /// NOLINT(performance-noexcept-swap)
    {
        /// Swap two PODArray objects, arr1 and arr2, that satisfy the following conditions:
        /// - The elements of arr1 are stored on stack.
        /// - The elements of arr2 are stored on heap.
        auto swap_stack_heap = [&](PODArrayOwning & arr1, PODArrayOwning & arr2)
        {
            size_t stack_size = arr1.size();
            size_t stack_allocated = arr1.allocated_bytes();

            size_t heap_size = arr2.size();
            size_t heap_allocated = arr2.allocated_bytes();

            /// Keep track of the stack content we have to copy.
            char * stack_c_start = arr1.c_start;

            /// arr1 takes ownership of the heap memory of arr2.
            arr1.c_start = arr2.c_start;
            arr1.c_end_of_storage = arr1.c_start + heap_allocated - arr2.pad_right - arr2.pad_left;
            arr1.c_end = arr1.c_start + BufferDetails::byte_size(heap_size, sizeof(T));

            /// Allocate stack space for arr2.
            arr2.alloc(stack_allocated, std::forward<TAllocatorParams>(allocator_params)...);
            /// Copy the stack content.
            memcpy(arr2.c_start, stack_c_start, BufferDetails::byte_size(stack_size, sizeof(T)));
            arr2.c_end = arr2.c_start + BufferDetails::byte_size(stack_size, sizeof(T));
        };

        auto do_move = [&](PODArrayOwning & src, PODArrayOwning & dest)
        {
            if (src.isAllocatedFromStack())
            {
                dest.dealloc();
                dest.alloc(src.allocated_bytes(), std::forward<TAllocatorParams>(allocator_params)...);
                memcpy(dest.c_start, src.c_start, BufferDetails::byte_size(src.size(), sizeof(T)));
                dest.c_end = dest.c_start + BufferDetails::byte_size(src.size(), sizeof(T));

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
        if (!this->isInitialized() && rhs.isInitialized())
        {
            do_move(rhs, *this);
            return;
        }
        if (this->isInitialized() && !rhs.isInitialized())
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

            this->c_end_of_storage = this->c_start + rhs_allocated - Base::pad_right - Base::pad_left;
            rhs.c_end_of_storage = rhs.c_start + lhs_allocated - Base::pad_right - Base::pad_left;

            this->c_end = this->c_start + BufferDetails::byte_size(rhs_size, sizeof(T));
            rhs.c_end = rhs.c_start + BufferDetails::byte_size(lhs_size, sizeof(T));
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
        this->resize_exact(n, std::forward<TAllocatorParams>(allocator_params)...);
        std::fill(Base::begin(), Base::end(), x);
    }

    template <typename It1, typename It2, typename... TAllocatorParams>
    void assign(It1 from_begin, It2 from_end, TAllocatorParams &&... allocator_params)
    {
        static_assert(memcpy_can_be_used_for_assignment<std::decay_t<T>, std::decay_t<decltype(*from_begin)>>);
        this->assertNotIntersects(from_begin, from_end);

        size_t required_capacity = from_end - from_begin;
        if (required_capacity > this->capacity())
            this->reserve_exact(required_capacity, std::forward<TAllocatorParams>(allocator_params)...);

        size_t bytes_to_copy = BufferDetails::byte_size(required_capacity, sizeof(T));
        if (bytes_to_copy)
            memcpy(this->c_start, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);

        this->c_end = this->c_start + bytes_to_copy;
    }

    // ISO C++ has strict ambiguity rules, thus we cannot apply TAllocatorParams here.
    void assign(const PODArrayOwning & from)
    {
        assign(from.begin(), from.end());
    }

    void erase(const_iterator first, const_iterator last)
    {
        // TODO: ask milovidov why
        iterator first_no_const = const_cast<iterator>(first);
        iterator last_no_const = const_cast<iterator>(last);

        size_t items_to_move = Base::end() - last;

        while (items_to_move != 0)
        {
            *first_no_const = *last_no_const;

            ++first_no_const;
            ++last_no_const;

            --items_to_move;
        }

        this->c_end = reinterpret_cast<char *>(first_no_const);
    }

    void erase(const_iterator pos)
    {
        this->erase(pos, pos + 1);
    }

};

}
