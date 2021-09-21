#pragma once

#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <utility>

/// Compile-time array that can be used as a template argument
template <class T, size_t N>
struct CTArray
{
    constexpr CTArray(const T(&ref)[N]) //NOLINT
    {
        std::copy_n(ref, N, storage);
    }

    constexpr T operator[](size_t i) const { return storage[i]; }

    static constexpr size_t size = N;

    T storage[N];
};

constexpr auto MakeCTArray(auto arg, auto ...args)
{
    return CTArray<decltype(arg), sizeof...(args) + 1>({arg, args...});
}
