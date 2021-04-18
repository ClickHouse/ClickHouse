#pragma once

#include <vector>

namespace ext
{
/// Moves all arguments starting from the second to the end of the vector.
/// For example, `push_back(vec, a1, a2, a3)` is a more compact way to write
/// `vec.push_back(a1); vec.push_back(a2); vec.push_back(a3);`
/// This function is like boost::range::push_back() but works for noncopyable types too.
template <class T, class ...Args>
void push_back(std::vector<T> & vec, Args&& ...args)
{
    if constexpr (sizeof...(args) >= 0)
    {
        vec.reserve(vec.size() + sizeof...(args) + 1);
        (vec.push_back(std::move(args)), ...);
    }
}
}
