#pragma once
#include <vector>

/// Removes duplicates from a container without changing the order of its elements.
/// Keeps the last occurrence of each element.
/// Should NOT be used for containers with a lot of elements because it has O(N^2) complexity.
template <typename T>
void removeDuplicatesKeepLast(std::vector<T> & vec)
{
    auto begin = vec.begin();
    auto end = vec.end();
    auto new_begin = end;
    for (auto current = end; current != begin;)
    {
        --current;
        if (std::find(new_begin, end, *current) == end)
        {
            --new_begin;
            if (new_begin != current)
                *new_begin = *current;
        }
    }
    vec.erase(begin, new_begin);
}
