#pragma once

#include <algorithm>
#include <functional>
#include <iterator>


namespace DB
{

/// Merges the adjacent sorted ranges `[begin, middle)` and `[middle, end)` in place, keeping the order of equal elements.
/// It works like `std::inplace_merge`, but takes the memory from `temp_buffer`, so a series of merges can share one allocation.
/// The buffer is grown if it is smaller than the shorter range, and it is never shrunk; its contents are not kept.
/// The shorter range is copied to the buffer, and the ranges are merged starting from its side, so no element is overwritten before it is read.
/// `TempBuffer` is any contiguous container with `size`, `resize` and `data`, such as `PODArray` or `VectorWithMemoryTracking`.
template <std::bidirectional_iterator Iterator, typename Less = std::less<>, typename TempBuffer>
void inplaceMergeWithTempBuffer(Iterator begin, Iterator middle, Iterator end, Less less, TempBuffer & temp_buffer)
{
    const size_t left_size = std::distance(begin, middle);
    const size_t right_size = std::distance(middle, end);
    const size_t min_buffer_size = std::min(left_size, right_size);
    if (temp_buffer.size() < min_buffer_size)
    {
        /// The contents are not kept, so the old memory is freed before the new one is allocated,
        /// unless the buffer has `capacity` and it is enough.
        bool has_enough_capacity = false;
        if constexpr (requires { temp_buffer.capacity(); })
            has_enough_capacity = temp_buffer.capacity() >= min_buffer_size;
        if (!has_enough_capacity)
            temp_buffer = TempBuffer{};
        /// `PODArray::resize` rounds the capacity up, which is not needed here.
        if constexpr (requires { temp_buffer.resize_exact(min_buffer_size); })
            temp_buffer.resize_exact(min_buffer_size);
        else
            temp_buffer.resize(min_buffer_size);
    }

    if (left_size <= right_size)
    {
        auto * left = temp_buffer.data();
        auto * left_end = std::copy(begin, middle, left);
        Iterator right = middle;
        Iterator out = begin;
        while (left != left_end && right != end)
            *out++ = less(*right, *left) ? *right++ : *left++;
        /// The rest of the right range is already in its place.
        std::copy(left, left_end, out);
    }
    else
    {
        /// `left` and `right` point after the last elements which are not merged yet.
        auto * right_begin = temp_buffer.data();
        auto * right = std::copy(middle, end, right_begin);
        Iterator left = middle;
        Iterator out = end;
        while (left != begin && right != right_begin)
        {
            Iterator last_left = std::prev(left);
            if (less(right[-1], *last_left))
            {
                *--out = *last_left;
                left = last_left;
            }
            else
            {
                *--out = *--right;
            }
        }
        /// The rest of the left range is already in its place.
        std::copy_backward(right_begin, right, out);
    }
}

/// The same, but the comparison is an object of type `Less` made by its default constructor.
template <std::bidirectional_iterator Iterator, typename Less = std::less<>, typename TempBuffer>
void inplaceMergeWithTempBuffer(Iterator begin, Iterator middle, Iterator end, TempBuffer & temp_buffer)
{
    inplaceMergeWithTempBuffer(begin, middle, end, Less{}, temp_buffer);
}

}
