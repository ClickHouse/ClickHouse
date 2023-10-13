#pragma once

#include <Core/Types.h>
#include <Common/BitHelpers.h>
#include <base/defines.h>
#include <array>
#include <iostream>


namespace
{
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

    /// Another example:

    /// first = 6:    00000110
    /// last = 7:     00000111
    /// first ^ last: 00000001
    /// mask:         00000000
    /// split = 11:   00001011

    UInt64 diff = first ^ last;
    UInt64 mask = toMask(diff) >> 1;

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

    intervalBinaryPartition(first, split, std::forward<F>(callback));
    if (split < last)
        intervalBinaryPartition(split + 1, last, std::forward<F>(callback));
}


/** Multidimensional version of binary space partitioning.
  * It takes a parallelogram - a direct product of intervals (in each dimension),
  * and splits it into smaller parallelograms - a direct product of partitions across each dimension.
  */
template <size_t N, size_t start_idx, typename F>
void parallelogramBinaryPartitionImpl(
    std::array<std::pair<UInt64, UInt64>, N> parallelogram,
    F && callback)
{
    intervalBinaryPartition(parallelogram[start_idx].first, parallelogram[start_idx].second,
        [&](UInt64 a, UInt64 b) mutable
        {
            auto new_parallelogram = parallelogram;
            new_parallelogram[start_idx].first = a;
            new_parallelogram[start_idx].second = b;

            if constexpr (start_idx + 1 < N)
                parallelogramBinaryPartitionImpl<N, start_idx + 1>(new_parallelogram, std::forward<F>(callback));
            else
                callback(new_parallelogram);
        });
}


template <size_t N, typename F>
void parallelogramBinaryPartition(
    std::array<std::pair<UInt64, UInt64>, N> parallelogram,
    F && callback)
{
    parallelogramBinaryPartitionImpl<N, 0>(parallelogram, std::forward<F>(callback));
}


/** Unpack an interval of Morton curve to parallelograms covered by it across N dimensions.
  */
template <size_t N, typename F>
void mortonIntervalToParallelograms(UInt64 first, UInt64 last, F && callback)
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


/** Given a parallelogram, find intervals of Morton curve that cover this parallelogram.
  * Note: to avoid returning too many intervals, the intervals can be returned larger than exactly needed
  * (covering some other points, not belonging to the parallelogram).
  */
template <size_t N, typename F>
void parallelogramToPossibleMortonIntervals(
    std::array<std::pair<UInt64, UInt64>, N> parallelogram,
    F && callback)
{
    parallelogramBinaryPartition<N>(parallelogram, [&](auto part)
    {
        size_t suffix_size = 0;
        for (size_t i = 0; i < N; ++i)
            if (part[i].second != part[i].first)
                suffix_size = std::max<size_t>(suffix_size,
                    bitScanReverse(part[i].second - part[i].first));

        UInt64 first = 0;
        UInt64 last = 0;

        size_t source_bit_idx = 0;
        size_t result_bit_idx = 0;

        while (result_bit_idx < 64)
        {
            for (size_t i = 0; i < N; ++i)
            {
                if (source_bit_idx <= suffix_size)
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

        callback(first, last);
    });
}
