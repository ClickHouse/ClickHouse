#pragma once

#include <vector>

/// Appends a specified vector with elements of another vector.
template <typename T>
void insertAtEnd(std::vector<T> & dest, const std::vector<T> & src)
{
    if (src.empty())
        return;
    dest.reserve(dest.size() + src.size());
    dest.insert(dest.end(), src.begin(), src.end());
}

template <typename T>
void insertAtEnd(std::vector<T> & dest, std::vector<T> && src)
{
    if (src.empty())
        return;
    if (dest.empty())
    {
        dest.swap(src);
        return;
    }
    dest.reserve(dest.size() + src.size());
    dest.insert(dest.end(), std::make_move_iterator(src.begin()), std::make_move_iterator(src.end()));
    src.clear();
}

template <typename Container>
void insertAtEnd(Container & dest, const Container & src)
{
    if (src.empty())
        return;

    dest.insert(dest.end(), src.begin(), src.end());
}

template <typename Container>
void insertAtEnd(Container & dest, Container && src)
{
    if (src.empty())
        return;
    if (dest.empty())
    {
        dest.swap(src);
        return;
    }

    dest.insert(dest.end(), std::make_move_iterator(src.begin()), std::make_move_iterator(src.end()));
    src.clear();
}
