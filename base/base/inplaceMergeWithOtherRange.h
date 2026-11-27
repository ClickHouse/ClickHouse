#pragma once

#include <algorithm>
#include <functional>
#include <iterator>

#include <base/defines.h>


/// Merges the sorted range `[other_begin, other_end)` into the sorted range `[begin, middle)`, which is followed by room for it:
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
///     inplaceMergeWithOtherRange(values.begin(), values.begin() + old_values_size, values.end(), other.begin(), other.end());
/// After that `values` is `{1, 2, 4, 7, 9}`.
template <std::bidirectional_iterator Iterator, std::bidirectional_iterator OtherIterator, typename Less = std::less<>>
void inplaceMergeWithOtherRange(Iterator begin, Iterator middle, Iterator end, OtherIterator other_begin, OtherIterator other_end, Less less = {})
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
