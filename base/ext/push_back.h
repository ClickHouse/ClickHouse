#pragma once

#include <vector>

namespace ext
{

/// Moves all arguments starting from the second to the end of the vector.
/// For example, `push_back(vec, a1, a2, a3)` is a more compact way to write
/// `vec.push_back(a1); vec.push_back(a2); vec.push_back(a3);`
/// This function is like boost::range::push_back() but works for noncopyable types too.
template <typename T>
void push_back(std::vector<T> &)
{
}

template <typename T, typename FirstArg, typename... OtherArgs>
void push_back(std::vector<T> & vec, FirstArg && first, OtherArgs &&... other)
{
    vec.reserve(vec.size() + sizeof...(other) + 1);
    vec.emplace_back(std::move(first));
    push_back(vec, std::move(other)...);
}

}
