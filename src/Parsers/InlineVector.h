#pragma once

#include <algorithm>
#include <cstddef>
#include <new>
#include <type_traits>

namespace DB
{

/** A vector of trivially copyable elements that keeps the first N of them inline.
  *
  * Exists so that the parser does not have to depend on `absl::InlinedVector`: the only thing it
  * ever needs is a short append-only list that does not allocate, and abseil brings in a large
  * amount of code for that. Deliberately minimal - grow it as new call sites appear.
  */
template <typename T, size_t N>
class InlineVector
{
    static_assert(std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>);

public:
    /// `inplace` is deliberately left uninitialized: only the first `count` of its elements are
    /// ever read, and not zeroing the rest is the whole point of keeping them inline.
    InlineVector() = default; /// NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)

    InlineVector(const InlineVector & other) { assign(other); }

    InlineVector & operator=(const InlineVector & other)
    {
        if (this != &other)
        {
            clear();
            assign(other);
        }
        return *this;
    }

    ~InlineVector()
    {
        if (heap)
            ::operator delete(heap);
    }

    const T * begin() const { return data(); }
    const T * end() const { return data() + count; }
    T * begin() { return data(); }
    T * end() { return data() + count; }

    size_t size() const { return count; }
    bool empty() const { return count == 0; }

    void clear() { count = 0; }

    void push_back(const T & value) /// NOLINT(readability-identifier-naming)
    {
        if (count == capacity)
            grow();
        data()[count] = value;
        ++count;
    }

private:
    T inplace[N];
    T * heap = nullptr;
    size_t count = 0;
    size_t capacity = N;

    T * data() { return heap ? heap : inplace; }
    const T * data() const { return heap ? heap : inplace; }

    void assign(const InlineVector & other)
    {
        while (capacity < other.count)
            grow();
        std::copy(other.begin(), other.end(), data());
        count = other.count;
    }

    void grow()
    {
        size_t new_capacity = capacity * 2;
        T * new_heap = static_cast<T *>(::operator new(new_capacity * sizeof(T)));
        std::copy(data(), data() + count, new_heap);
        if (heap)
            ::operator delete(heap);
        heap = new_heap;
        capacity = new_capacity;
    }
};

}
