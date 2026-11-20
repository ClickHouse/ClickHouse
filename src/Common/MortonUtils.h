#pragma once

#include <Core/Types.h>
#include <Common/BitHelpers.h>
#include <base/defines.h>
#include <array>
#include <set>


/** Functions to analyze the Morton space-filling curve on ranges.
  * There are two operations:
  *
  * 1. Inverting the Morton curve on a range.
  * Given a range of values of Morton curve,
  *   mortonEncode(x, y) in [a, b]
  * get possible set of values of its arguments.
  * This set is represented by a set of hyperrectangles in (x, y) space.
  *
  * 2. Calculating the Morton curve on a hyperrectangle.
  * Given a hyperrectangle in (x, y) space
  *   (x, y) in [x_min, x_max] × [y_min, y_max]
  * get possible intervals of the mortonEncode(x, y).
  *
  * These operations could be used for index analysis.
  *
  * Note: currently it is only tested in 2d.
  */


namespace impl
{
    /// After the most significant bit 1, set all subsequent less significant bits to 1 as well.
    inline UInt64 toMask(UInt64 n)
    {
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n;
    }
}


/** Splits the interval [first, last] to a set of intervals [first_i, last_i],
  * each of them determined by a bit prefix: [xxxxxx0000, xxxxxx1111],
  *
  * For example, the interval [6, 13] = {5, 7, 8, 9, 10, 11, 12, 13}
  * will be represented by the set of intervals:
  * - [6,  7]   0000011*
  * - [8,  11]  000010**
  * - [12, 13]  0000110*
  *
  * It means that if you have a binary space partition by powers of two,
  * every of the resulting intervals will fully occupy one of the levels of this partition.
  */
template <typename F>
void intervalBinaryPartition(UInt64 first, UInt64 last, F && callback)
{
    /// first = 6:    00000110
    /// last = 13:    00001101
    /// first ^ last: 00001011
    /// mask:         00000111
    /// split = 7:    00000111

    /// first = 8:    00001000
    /// last = 13:    00001101
    /// first ^ last: 00000101
    /// mask:         00000011
    /// split = 11:   00001011

    /// first = 8:    00001000
    /// last = 11:    00001011
    /// first ^ last: 00000011
    /// mask:         00000001
    /// split = 9:    00001001

    /// Another example:

    /// first = 15:   00001111
    /// last = 31:    00011111
    /// first ^ last: 00010000
    /// mask:         00001111
    /// split = 15:   00001111

    UInt64 diff = first ^ last;
    UInt64 mask = impl::toMask(diff) >> 1;

    /// The current interval represents a whole range with fixed prefix.
    if ((first & mask) == 0 && (last & mask) == mask)
    {
        chassert(((last - first + 1) & (last - first)) == 0); /// The interval length is one less than a power of two.
        callback(first, last);
        return;
    }

    UInt64 split = first | mask;

    chassert(split >= first);
    chassert(split <= last);

    intervalBinaryPartition(first, split, callback);
    if (split < last)
        intervalBinaryPartition(split + 1, last, callback);
}


/** Multidimensional version of binary space partitioning.
  * It takes a hyperrectangle - a direct product of intervals (in each dimension),
  * and splits it into smaller hyperrectangles - a direct product of partitions across each dimension.
  */
template <size_t N, size_t start_idx, typename F>
void hyperrectangleBinaryPartitionImpl(
    std::array<std::pair<UInt64, UInt64>, N> hyperrectangle,
    F && callback)
{
    intervalBinaryPartition(hyperrectangle[start_idx].first, hyperrectangle[start_idx].second,
        [&](UInt64 a, UInt64 b) mutable
        {
            auto new_hyperrectangle = hyperrectangle;
            new_hyperrectangle[start_idx].first = a;
            new_hyperrectangle[start_idx].second = b;

            if constexpr (start_idx + 1 < N)
                hyperrectangleBinaryPartitionImpl<N, start_idx + 1>(new_hyperrectangle, std::forward<F>(callback));
            else
                callback(new_hyperrectangle);
        });
}


template <size_t N, typename F>
void hyperrectangleBinaryPartition(
    std::array<std::pair<UInt64, UInt64>, N> hyperrectangle,
    F && callback)
{
    hyperrectangleBinaryPartitionImpl<N, 0>(hyperrectangle, std::forward<F>(callback));
}


/** Unpack an interval of Morton curve to hyperrectangles covered by it across N dimensions.
  */
template <size_t N, typename F>
void mortonIntervalToHyperrectangles(UInt64 first, UInt64 last, F && callback)
{
    intervalBinaryPartition(first, last, [&](UInt64 a, UInt64 b)
    {
        std::array<std::pair<UInt64, UInt64>, N> unpacked{};

        for (size_t bit_idx = 0; bit_idx < 64; ++bit_idx)
        {
            size_t source_bit = 63 - bit_idx;
            size_t result_bit = (63 - bit_idx) / N;

            unpacked[source_bit % N].first |= ((a >> source_bit) & 1) << result_bit;
            unpacked[source_bit % N].second |= ((b >> source_bit) & 1) << result_bit;
        }

        callback(unpacked);
    });
}


/** Given a hyperrectangle, find intervals of Morton curve that cover this hyperrectangle.
  * Note: to avoid returning too many intervals, the intervals can be returned larger than exactly needed
  * (covering some other points, not belonging to the hyperrectangle).
  * We do it by extending hyperrectangles to hypercubes.
  */
template <size_t N, typename F>
void hyperrectangleToPossibleMortonIntervals(
    std::array<std::pair<UInt64, UInt64>, N> hyperrectangle,
    F && callback)
{
    /// Due to extension to cubes, there could be duplicates. Filter them.
    std::set<std::pair<UInt64, UInt64>> found_intervals;

    hyperrectangleBinaryPartition<N>(hyperrectangle, [&](auto part)
    {
        size_t suffix_size = 0;
        for (size_t i = 0; i < N; ++i)
            if (part[i].second != part[i].first)
                suffix_size = std::max<size_t>(suffix_size,
                    1 + bitScanReverse(part[i].second ^ part[i].first));

        UInt64 first = 0;
        UInt64 last = 0;

        size_t source_bit_idx = 0;
        size_t result_bit_idx = 0;

        while (result_bit_idx < 64)
        {
            for (size_t i = 0; i < N; ++i)
            {
                if (source_bit_idx < suffix_size)
                {
                    last |= (1 << result_bit_idx);
                }
                else
                {
                    UInt64 bit = (((part[i].first >> source_bit_idx) & 1) << result_bit_idx);
                    first |= bit;
                    last |= bit;
                }

                ++result_bit_idx;
                if (!(result_bit_idx < 64))
                    break;
            }
            ++source_bit_idx;
        }

        if (found_intervals.insert({first, last}).second)
            callback(first, last);
    });
}
