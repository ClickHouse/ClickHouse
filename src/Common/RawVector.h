#pragma once

#include <vector>
#include <memory>
#include <type_traits>
#include <Common/Allocator.h>
#include <Core/Defines.h> /// PADDING_FOR_SIMD
#include <base/defines.h> /// ASAN_UNPOISON_MEMORY_REGION() noop/ADDRESS_SANITIZER
#if __has_include(<sanitizer/asan_interface.h>) && defined(ADDRESS_SANITIZER)
#   include <sanitizer/asan_interface.h> /// ASAN_UNPOISON_MEMORY_REGION()
#endif

namespace DB
{

// Allocator adaptor that interposes construct() calls to
// convert value initialization into default initialization.
//
// Based on: https://stackoverflow.com/a/21028912/328260
//
// Also the similar allocator is used by StarRocks database.
//
// NOTE: we may get rid of std::allocator<> inheritance (for better visibility).
template <typename T, typename A = std::allocator<T>>
class DefaultInitAllocator : public A
{
    static inline constexpr size_t alignSize(size_t size)
    {
        return ((size + sizeof(T) - 1) / sizeof(T)) * sizeof(T);
    }

    static constexpr size_t MEMORY_PADDING = alignSize(PADDING_FOR_SIMD);
    static constexpr size_t ELEMENT_PADDING = MEMORY_PADDING / sizeof(T);


public:
    using A::A;

    /// NOTE: ClickHouse uses 20 C++ standard, and:
    /// - allocate(size_t n, const void * hint) -- deprecated in c++20
    /// - allocate_at_least(size_t n)           -- since c++23

    [[nodiscard]] T * allocate(size_t n)
    {
        size_t padded_n = n + ELEMENT_PADDING * 2;
        T * ptr = reinterpret_cast<T *>(allocator.alloc(padded_n * sizeof(T)));
        if (unlikely(!ptr))
            return ptr;
        ASAN_UNPOISON_MEMORY_REGION(ptr, sizeof(T) * padded_n);
        T * padded_ptr = ptr + ELEMENT_PADDING;
        return padded_ptr;
    }

    void deallocate(T * ptr, size_t n) noexcept
    {
        T * unpadded_ptr = ptr - ELEMENT_PADDING;
        size_t padded_n = n + ELEMENT_PADDING * 2;
        allocator.free(reinterpret_cast<void *>(unpadded_ptr), padded_n * sizeof(T));
        ASAN_POISON_MEMORY_REGION(unpadded_ptr, sizeof(T) * padded_n);
    }

    template <typename U>
    void construct(U * ptr) noexcept(std::is_nothrow_default_constructible<U>::value)
    {
        ::new (static_cast<void *>(ptr)) U;
    }

private:
    Allocator<false, false> allocator;
};

template <class T>
class RawVector : public std::vector<T, DefaultInitAllocator<T>>
{
    using vector_no_default_init = std::vector<T>;
    using vector = std::vector<T, DefaultInitAllocator<T>>;

public:
    using vector::vector;

    size_t allocated_bytes() const { return this->capacity() * sizeof(T); }

    /// Avoid debug assertion, since we have padding for SIMD_BYTES.
    constexpr typename vector::reference operator[](size_t pos) { return this->begin()[pos]; }
    constexpr typename vector::const_reference operator[](size_t pos) const { return this->begin()[pos]; }

    void resize_fill(size_t size)
    {
        this->reserve(size);
        reinterpret_cast<vector_no_default_init *>(this)->resize(size);
    }

    void resize_assume_reserved(size_t size)
    {
        return this->resize(size);
    }

    template <class InputIt>
    void insert(InputIt first, InputIt last)
    {
        get_stl_vector()->insert(this->end(), first, last);
    }

    template <typename InputIt>
    void insert_assume_reserved(InputIt first, InputIt last)
    {
        get_stl_vector()->insert(this->end(), first, last);
    }

    /// FIXME: Noop
    void protect() const {}

private:
    vector * get_stl_vector() { return static_cast<vector *>(this); }
};

}
