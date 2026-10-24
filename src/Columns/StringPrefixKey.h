#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumnImpl.h>
#include <Common/PODArray.h>
#include <Common/transformEndianness.h>
#include <base/defines.h>
#include <base/types.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <string_view>


namespace DB
{

/** Abbreviated keys for sorting strings.
  *
  * `stringPrefixKey` maps the first 8 bytes of a string to a big-endian number, padded with zero bytes on the right.
  * If `stringPrefixKey(a) < stringPrefixKey(b)`, then `a < b` in `memcmp` order: a string shorter than 8 bytes is padded
  * with zeros, so it can only tie with its own extensions, never compare greater than them. A sort can therefore
  * compare these numbers first and compare whole strings only when the numbers are equal. Equal strings have equal keys,
  * so stable sorting and the equal ranges of `updatePermutation` are not affected.
  *
  * The numbers are compared with each other in a contiguous array instead of comparing two strings at random places
  * in memory, which removes most of the cache misses of a string sort.
  *
  * If all strings to sort share a common prefix (URLs, zero-padded numbers, values like `prefix_N`), the keys are taken
  * after it: the order of the strings is then the order of their remainders, and the keys stay distinct.
  */
ALWAYS_INLINE inline UInt64 stringPrefixKey(const UInt8 * data, size_t size)
{
    /// Reads 8 bytes even if the string is shorter: the data must be followed by readable padding, as in `PaddedPODArray`.
    /// The bytes past the end of the string are masked out before anything depends on them.
    UInt64 key = unalignedLoad<UInt64>(data);
    transformEndianness<std::endian::big>(key);
    UInt64 mask = size >= sizeof(key) ? ~0ULL : ~(~0ULL >> (size * 8));
    return key & mask;
}

/// Prefix key of `value` after its first `offset` bytes.
ALWAYS_INLINE inline UInt64 stringPrefixKey(std::string_view value, size_t offset)
{
    return stringPrefixKey(reinterpret_cast<const UInt8 *>(value.data()) + offset, value.size() - offset);
}

/// Length of the common prefix of `a` and `b`, but not more than `limit`, which must not exceed their sizes.
/// Reads up to 7 bytes past `limit`, which must be readable, as in `PaddedPODArray`.
ALWAYS_INLINE inline size_t commonPrefixLength(const UInt8 * a, const UInt8 * b, size_t limit)
{
    size_t i = 0;
    while (i < limit)
    {
        UInt64 diff = unalignedLoad<UInt64>(a + i) ^ unalignedLoad<UInt64>(b + i);
        size_t remaining = limit - i;
        if (remaining < sizeof(diff))
        {
            /// Ignore the bytes past `limit` before branching on them.
            if constexpr (std::endian::native == std::endian::little)
                diff &= (1ULL << (remaining * 8)) - 1;
            else
                diff &= ~(~0ULL >> (remaining * 8));
        }
        if (diff)
        {
            if constexpr (std::endian::native == std::endian::little)
                return i + std::countr_zero(diff) / 8;
            else
                return i + std::countl_zero(diff) / 8;
        }
        i += sizeof(diff);
    }
    return limit;
}

/** Getters of the strings of the rows of a column, for `PrefixKeySort`:
  * - `get(row)` returns the bytes of the string of a row, followed by readable padding;
  * - `key(row, offset)` returns the prefix key of the string of a row after the first `offset` bytes,
  *   where `offset` does not exceed the length of the common prefix of the strings to sort.
  */

/// Strings of `ColumnString`, read directly from its `chars` and `offsets`.
struct StringPrefixKeyGetter
{
    const UInt8 * chars;
    /// `offsets[-1]` must be readable and equal to zero, as in `ColumnString`.
    const UInt64 * offsets;

    ALWAYS_INLINE std::string_view get(size_t row) const
    {
        UInt64 offset = offsets[static_cast<ssize_t>(row) - 1];
        return {reinterpret_cast<const char *>(chars + offset), offsets[row] - offset};
    }

    ALWAYS_INLINE UInt64 key(size_t row, size_t offset) const { return stringPrefixKey(get(row), offset); }
};

/// Strings of `ColumnFixedString`.
struct FixedStringPrefixKeyGetter
{
    const UInt8 * chars;
    size_t n;

    ALWAYS_INLINE std::string_view get(size_t row) const { return {reinterpret_cast<const char *>(chars + row * n), n}; }
    ALWAYS_INLINE UInt64 key(size_t row, size_t offset) const { return stringPrefixKey(get(row), offset); }
};

inline StringPrefixKeyGetter prefixKeyGetter(const ColumnString & column)
{
    return {column.getChars().data(), column.getOffsets().data()};
}

inline FixedStringPrefixKeyGetter prefixKeyGetter(const ColumnFixedString & column)
{
    return {column.getChars().data(), column.getN()};
}

struct CommonPrefix
{
    size_t length = 0;
    /// All strings are equal to the common prefix.
    bool all_equal = false;
};

/// Common prefix of the strings of the rows `[begin, end)`, which must not be empty.
template <typename RandomIt, typename GetString>
CommonPrefix commonPrefixOfRows(RandomIt begin, RandomIt end, const GetString & get_string)
{
    std::string_view first = get_string.get(*begin);
    const auto * first_data = reinterpret_cast<const UInt8 *>(first.data());
    size_t length = first.size();
    size_t max_size = first.size();
    RandomIt it = begin + 1;
    for (; it != end && length; ++it)
    {
        std::string_view value = get_string.get(*it);
        length = commonPrefixLength(first_data, reinterpret_cast<const UInt8 *>(value.data()), std::min(length, value.size()));
        max_size = std::max(max_size, value.size());
    }
    /// If the loop stopped early, some strings were not looked at, and not all strings are known to be equal.
    return {length, it == end && max_size == length};
}

/** `Sort` and `PartialSort` for `IColumn::getPermutationImpl`, `IColumn::updatePermutationImpl` and similar code, that
  * sort rows by the prefix keys of their strings first and call `compare` only when the keys are equal. `compare` keeps
  * its role: it compares whole values and defines the stability. `ascending` must match its direction, and the keys
  * of `GetString` must be consistent with its order.
  */
template <bool ascending, typename GetString>
class PrefixKeySort
{
public:
    explicit PrefixKeySort(GetString get_string_) : get_string(get_string_) {}

    /// The buffer is only scratch space, so a copy starts with its own empty buffer.
    PrefixKeySort(const PrefixKeySort & other) : get_string(other.get_string) {}

    template <typename RandomIt, typename Compare>
    void operator()(RandomIt begin, RandomIt end, Compare compare)
    {
        sortImpl(begin, end, end, compare);
    }

    template <typename RandomIt, typename Compare>
    void operator()(RandomIt begin, RandomIt middle, RandomIt end, Compare compare)
    {
        sortImpl(begin, middle, end, compare);
    }

private:
    /// For very small ranges filling the buffer costs more than it saves.
    static constexpr size_t min_size_to_use_keys = 16;
    /// A partial sort of a few elements does O(n) comparisons, about as many as filling the buffer takes,
    /// so the keys are used only if at least 1/16 of the range is sorted.
    static constexpr size_t min_partial_sort_fraction_to_use_keys = 16;

    struct KeyWithIndex
    {
        UInt64 key;
        size_t index;
    };

    GetString get_string;
    PaddedPODArray<KeyWithIndex> buffer;

    /// Sorts `[begin, end)` if it is sorted or strictly reverse sorted, which are common inputs. Checking for them takes
    /// fewer comparisons than filling the buffer, and on other inputs the checks usually stop at the first few elements.
    template <typename RandomIt, typename Compare>
    static bool trySortSortedOrReversed(RandomIt begin, RandomIt end, Compare compare)
    {
        if (std::is_sorted(begin, end, compare))
            return true;
        /// Strictly, so that reversing keeps the order of equal elements.
        if (std::adjacent_find(begin, end, [&](auto lhs, auto rhs) { return !compare(rhs, lhs); }) == end)
        {
            std::reverse(begin, end);
            return true;
        }
        return false;
    }

    template <typename RandomIt, typename Compare>
    void sortImpl(RandomIt begin, RandomIt middle, RandomIt end, Compare compare)
    {
        const size_t size = end - begin;
        const bool is_partial = middle != end;

        if (size < min_size_to_use_keys
            || (is_partial && static_cast<size_t>(middle - begin) * min_partial_sort_fraction_to_use_keys < size))
        {
            DefaultSort()(begin, middle, end, compare);
            return;
        }

        if (!is_partial && trySortSortedOrReversed(begin, end, compare))
            return;

        const CommonPrefix common_prefix = commonPrefixOfRows(begin, end, get_string);

        /// All keys would be equal.
        if (common_prefix.all_equal)
        {
            DefaultSort()(begin, middle, end, compare);
            return;
        }

        buffer.resize(size);
        for (size_t i = 0; i < size; ++i)
            buffer[i] = {get_string.key(begin[i], common_prefix.length), begin[i]};

        auto key_less = [&](const KeyWithIndex & lhs, const KeyWithIndex & rhs)
        {
            if (lhs.key != rhs.key)
                return ascending ? lhs.key < rhs.key : lhs.key > rhs.key;
            return compare(lhs.index, rhs.index);
        };
        DefaultSort()(buffer.begin(), buffer.begin() + (middle - begin), buffer.end(), key_less);

        /// Write back all indices, not only the sorted part, so that the result stays a permutation.
        for (size_t i = 0; i < size; ++i)
            begin[i] = buffer[i].index;
    }
};

template <bool ascending, typename GetString>
PrefixKeySort<ascending, GetString> makePrefixKeySort(GetString get_string)
{
    return PrefixKeySort<ascending, GetString>(get_string);
}

}
