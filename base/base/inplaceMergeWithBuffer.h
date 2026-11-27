#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>

#include <base/defines.h>


namespace detail
{

/// Merges two sorted ranges into `[begin, end)`: the left one is `[other_begin, other_end)` in another buffer,
/// and the right one is `[middle, end)`, already in its place. `[begin, middle)` has as many elements as the left range.
/// The result is written starting from the beginning, so no element is overwritten before it is read.
template <std::forward_iterator Iterator, std::forward_iterator OtherIterator, typename Less>
void inplaceMergeWithLeftPartInOtherBuffer(Iterator begin, Iterator middle, Iterator end, OtherIterator other_begin, OtherIterator other_end, Less less)
{
    chassert(std::distance(begin, middle) == std::distance(other_begin, other_end));

    OtherIterator left = other_begin;
    Iterator right = middle;
    Iterator out = begin;
    while (left != other_end && right != end)
        *out++ = less(*right, *left) ? *right++ : *left++;
    /// The rest of `[middle, end)` is already in its place.
    std::copy(left, other_end, out);
}

/// Merges two sorted ranges into `[begin, end)`: the left one is `[begin, middle)`, already in its place,
/// and the right one is `[other_begin, other_end)` in another buffer. `[middle, end)` has as many elements as the right range.
/// The result is written starting from the end, so no element is overwritten before it is read.
template <std::bidirectional_iterator Iterator, std::bidirectional_iterator OtherIterator, typename Less>
void inplaceMergeWithRightPartInOtherBuffer(Iterator begin, Iterator middle, Iterator end, OtherIterator other_begin, OtherIterator other_end, Less less)
{
    chassert(std::distance(middle, end) == std::distance(other_begin, other_end));

    /// `left` and `right` point after the last elements which are not merged yet.
    Iterator left = middle;
    OtherIterator right = other_end;
    Iterator out = end;
    while (left != begin && right != other_begin)
    {
        Iterator last_left = std::prev(left);
        OtherIterator last_right = std::prev(right);
        if (less(*last_right, *last_left))
        {
            *--out = *last_left;
            left = last_left;
        }
        else
        {
            *--out = *last_right;
            right = last_right;
        }
    }
    /// The rest of `[begin, middle)` is already in its place.
    std::copy_backward(other_begin, right, out);
}

}


/// Merges the adjacent sorted ranges `[begin, middle)` and `[middle, end)` in place, keeping the order of equal elements.
/// The shorter range is copied to `temp_buffer`, and the ranges are merged starting from its side, so no element is overwritten before it is read.
/// The buffer is grown if it is smaller than the shorter range, and it is never shrunk; its contents are not kept.
/// `TempBuffer` is any contiguous container with `size`, `resize` and `data`, such as `PODArray` or `VectorWithMemoryTracking`.
///
/// The difference from `std::inplace_merge`: `std::inplace_merge` allocates a temporary buffer of its own in every call,
/// while here a series of merges can share one buffer, and the caller chooses how its memory is allocated.
///
/// Example:
///     std::vector<int> values = {5, 6, 7, 8, 1, 2};
///     size_t left_size = 4;
///     std::vector<int> temp_buffer;
///     inplaceMergeWithTempBuffer(values.begin(), values.begin() + left_size, values.end(), temp_buffer);
/// After that `values` is `{1, 2, 5, 6, 7, 8}`, and `temp_buffer` can be passed to the next merge.
template <std::bidirectional_iterator Iterator, typename TempBuffer, typename Less = std::less<>>
void inplaceMergeWithTempBuffer(Iterator begin, Iterator middle, Iterator end, TempBuffer & temp_buffer, Less less = {})
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
        auto * left_begin = temp_buffer.data();
        auto * left_end = std::copy(begin, middle, left_begin);
        detail::inplaceMergeWithLeftPartInOtherBuffer(begin, middle, end, left_begin, left_end, less);
    }
    else
    {
        auto * right_begin = temp_buffer.data();
        auto * right_end = std::copy(middle, end, right_begin);
        detail::inplaceMergeWithRightPartInOtherBuffer(begin, middle, end, right_begin, right_end, less);
    }
}


/// Merges the sorted range `[other_begin, other_end)` of another buffer into the sorted range `[begin, middle)`, which is followed by room for it:
/// `[middle, end)` has as many elements as the other range, and its contents are overwritten.
/// The result is written starting from the end, so no element is overwritten before it is read and no other memory is needed.
/// The order of equal elements is kept, and those of `[begin, middle)` go first.
///
/// Differences from the standard algorithms:
/// - `std::inplace_merge` merges only two ranges standing next to each other, so the other range would have to be copied
///   to `[middle, end)` first, and then it allocates a temporary buffer for the merge.
/// - `std::merge` needs separate memory for the whole result, because its output must not overlap its input ranges.
///
/// Example:
///     std::vector<int> values = {1, 4, 7};
///     std::vector<int> other = {2, 9};
///     size_t old_values_size = values.size();
///     values.resize(old_values_size + other.size());
///     inplaceMergeWithOtherBuffer(values.begin(), values.begin() + old_values_size, values.end(), other.begin(), other.end());
/// After that `values` is `{1, 2, 4, 7, 9}`.
template <std::bidirectional_iterator Iterator, std::bidirectional_iterator OtherIterator, typename Less = std::less<>>
void inplaceMergeWithOtherBuffer(Iterator begin, Iterator middle, Iterator end, OtherIterator other_begin, OtherIterator other_end, Less less = {})
{
    detail::inplaceMergeWithRightPartInOtherBuffer(begin, middle, end, other_begin, other_end, less);
}
