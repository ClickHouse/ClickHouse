#pragma once

#include <cstddef>
#include <memory>

#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/BitHelpers.h>
#include <Columns/BufferFWD.h>

#ifndef NDEBUG
#include <sys/mman.h>
#endif

namespace DB
{

// constexpr size_t integerRoundUp(size_t value, size_t dividend);

namespace BufferDetails
{

void protectMemoryRegion(void * addr, size_t len, int prot);

/// The amount of memory occupied by the num_elements of the elements.
size_t byte_size(size_t num_elements, size_t element_size); /// NOLINT

/// Minimum amount of memory to allocate for num_elements, including padding.
size_t minimum_memory_for_elements(size_t num_elements, size_t element_size, size_t pad_left, size_t pad_right); /// NOLINT

};

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_> // NOLINT
class IBuffer : public PODArray<T, initial_bytes, TAllocator, pad_right_, pad_left_>
{
protected:
    using PODBase = PODArray<T, initial_bytes, TAllocator, pad_right_, pad_left_>;
    static constexpr size_t element_size = sizeof(T);
    /// Round padding up to an whole number of elements to simplify arithmetic.
    static constexpr size_t pad_right = integerRoundUp(pad_right_, element_size);
    /// pad_left is also rounded up to 16 bytes to maintain alignment of allocated memory.
    static constexpr size_t pad_left = integerRoundUp(integerRoundUp(pad_left_, element_size), 16);

    // If we are using allocator with inline memory, the minimal size of
    // array must be in sync with the size of this memory.
    static_assert(allocatorInitialBytes<TAllocator> == 0
                  || allocatorInitialBytes<TAllocator> == initial_bytes);

    T * t_start()                      { return reinterpret_cast<T *>(this->c_start); } /// NOLINT
    T * t_end()                        { return reinterpret_cast<T *>(this->c_end); } /// NOLINT

    const T * t_start() const          { return reinterpret_cast<const T *>(this->c_start); } /// NOLINT
    const T * t_end() const            { return reinterpret_cast<const T *>(this->c_end); } /// NOLINT

    virtual void alloc_for_num_elements(size_t) {}
    virtual void realloc(size_t) {}
    virtual void dealloc() {}
    virtual void alloc(size_t) {}

    bool isInitialized() const
    {
        return PODBase::isInitialized();
    }

    bool isAllocatedFromStack() const
    {
        return PODBase::isAllocatedFromStack();
    }

public:
    using value_type = T;
    using iterator = T *;
    using const_iterator = const T *;

    virtual ~IBuffer() = default;
    IBuffer() = default;
    IBuffer(const IBuffer &) = delete;

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

    T * data() { return t_start(); }
    const T * data() const { return t_start(); }

    /// The index is signed to access -1th element without pointer overflow.
    T & operator[] (ssize_t n)
    {
        /// <= size, because taking address of one element past memory range is Ok in C++ (expression like &arr[arr.size()] is perfectly valid).
        assert((n >= (static_cast<ssize_t>(pad_left) ? -1 : 0)) && (n <= static_cast<ssize_t>(this->size())));
        return t_start()[n];
    }

    const T & operator[] (ssize_t n) const
    {
        assert((n >= (static_cast<ssize_t>(pad_left) ? -1 : 0)) && (n <= static_cast<ssize_t>(this->size())));
        return t_start()[n];
    }

    bool empty() const { return PODBase::empty(); }
    size_t size() const { return PODBase::size(); }
    size_t capacity() const { return PODBase::capacity(); }

    /// This method is safe to use only for information about memory usage.
    size_t allocated_bytes() const { return PODBase::allocated_bytes(); } /// NOLINT

    void clear() { PODBase::clear(); }

    virtual std::shared_ptr<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>> getOwningBuffer() = 0;

    void resize_assume_reserved(const size_t n) /// NOLINT
    {
        this->c_end = this->c_start + BufferDetails::byte_size(n, element_size);
    }

    const char * rawData() const
    {
        return this->c_start;
    }

    bool operator== (const IBuffer & rhs) const
    {
        if (this->size() != rhs.size())
            return false;

        const_iterator lhs_it = begin();
        const_iterator rhs_it = rhs.begin();

        while (lhs_it != end())
        {
            if (*lhs_it != *rhs_it)
                return false;

            ++lhs_it;
            ++rhs_it;
        }

        return true;
    }

    bool operator!= (const IBuffer & rhs) const
    {
        return !operator==(rhs);
    }
};

}
