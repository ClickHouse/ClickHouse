#pragma once

#include <algorithm>
#include <array>
#include <cstddef>

/// Compile-time array that can be used as a template argument.
template <class T, size_t N>
struct CTArray
{
    constexpr explicit CTArray(const std::array<T, N> & arr) { storage = arr; }

    constexpr T operator[](size_t i) const { return storage[i]; }

    constexpr auto begin() const { return storage.begin(); }
    constexpr auto end() const { return storage.end(); }

    constexpr bool contains(T value) const
    {
        return std::find(begin(), end(), value) != end();
    }

    static constexpr size_t size = N;

    std::array<T, N> storage;
};

template <class T, size_t N, size_t U>
constexpr CTArray<T, N + U> operator||(const CTArray<T, N> & arr, const CTArray<T, U> & other)
{
    std::array<T, N + U> st;
    std::copy_n(arr.begin(), N, st.begin());
    std::copy_n(other.begin(), U, st.begin() + N);

    return CTArray<T, N + U>(st);
}

constexpr auto MakeCTArray(auto arg, auto ...args)
    requires(std::is_same_v<decltype(arg), decltype(args)> && ...)
{
    return CTArray<decltype(arg), sizeof...(args) + 1>({arg, args...});
}
