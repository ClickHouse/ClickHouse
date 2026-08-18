#pragma once

#include <Core/Types.h>
#include <Common/BitHelpers.h>
#include <base/types.h>
#include <Functions/hilbertDecode2DLUT.h>
#include <base/defines.h>
#include <array>
#include <set>


namespace HilbertDetails
{

    struct Segment // represents [begin; end], all bounds are included
    {
        UInt64 begin;
        UInt64 end;
    };

}

/*
    Given the range of values of hilbert code - and this function will return segments of the Hilbert curve
    such that each of them lies in a whole domain (aka square)
           0                 1
    ┌────────────────────────────────┐
    │               │                │
    │               │                │
  0 │    00xxx      │     11xxx      │
    │      |        │       |        │
    │      |        │       |        │
    │_______________│________________│
    │      |        │       |        │
    │      |        │       |        │
    │      |        │       |        │
  1 │    01xxx______│_____10xxx      │
    │               │                │
    │               │                │
    └────────────────────────────────┘
    Imagine a square, one side of which is a x-axis, other is a y-axis.
    First approximation of the Hilbert curve is on the picture - U curve.
    So we divide Hilbert Code Interval on 4 parts each of which is represented by a square
    and look where the given interval [start, finish] is located:
    [00xxxxxx      |      01xxxxxx      |      10xxxxxx      |      11xxxxxx      ]
    1:     [                                                ]
           start = 0010111                                  end = 10111110
    2:     [       ]                    [                   ]
    If it contains a whole sector (that represents a domain=square),
    then we take this range. In the example above - it is a sector [01000000, 01111111]
    Then we dig into the recursion and check the remaining ranges.
    Note that after the first call all other ranges in the recursion will have either start or finish on the end of a range,
    so the complexity of the algorithm will be O(logN), where N is the maximum of hilbert code.
*/
template <typename F>
void segmentBinaryPartition(UInt64 start, UInt64 finish, UInt8 current_bits, F && callback)
{
    if (current_bits == 0)
        return;

    const auto next_bits = current_bits - 2;
    const auto history = current_bits == 64 ? 0 : (start >> current_bits) << current_bits;

    const auto chunk_mask = 0b11;
    const auto start_chunk = (start >> next_bits) & chunk_mask;
    const auto finish_chunk = (finish >> next_bits) & chunk_mask;

    auto construct_range = [next_bits, history](UInt64 chunk)
    {
        return HilbertDetails::Segment{
            .begin = history + (chunk << next_bits),
            .end = history + ((chunk + 1) << next_bits) - 1
        };
    };

    if (start_chunk == finish_chunk)
    {
        if ((finish - start + 1) == (1 << next_bits)) // it means that [begin, end] is a range
        {
            callback(HilbertDetails::Segment{.begin = start, .end = finish});
            return;
        }
        segmentBinaryPartition(start, finish, next_bits, callback);
        return;
    }

    for (auto range_chunk = start_chunk + 1; range_chunk < finish_chunk; ++range_chunk)
    {
        callback(construct_range(range_chunk));
    }

    const auto start_range = construct_range(start_chunk);
    if (start == start_range.begin)
    {
        callback(start_range);
    }
    else
    {
        segmentBinaryPartition(start, start_range.end, next_bits, callback);
    }

    const auto finish_range = construct_range(finish_chunk);
    if (finish == finish_range.end)
    {
        callback(finish_range);
    }
    else
    {
        segmentBinaryPartition(finish_range.begin, finish, next_bits, callback);
    }
}

// Given 2 points representing ends of the range of Hilbert Curve that lies in a whole domain.
// The are neighbour corners of some square - and the function returns ranges of both sides of this square
inline std::array<std::pair<UInt64, UInt64>, 2> createRangeFromCorners(UInt64 x1, UInt64 y1, UInt64 x2, UInt64 y2)
{
    UInt64 dist_x = x1 > x2 ? x1 - x2 : x2 - x1;
    UInt64 dist_y = y1 > y2 ? y1 - y2 : y2 - y1;
    UInt64 range_size = std::max(dist_x, dist_y);
    bool contains_minimum_vertice = x1 % (range_size + 1) == 0;
    if (contains_minimum_vertice)
    {
        UInt64 x_min = std::min(x1, x2);
        UInt64 y_min = std::min(y1, y2);
        return {
            std::pair<UInt64, UInt64>{x_min, x_min + range_size},
            std::pair<UInt64, UInt64>{y_min, y_min + range_size}
        };
    }

    UInt64 x_max = std::max(x1, x2);
    UInt64 y_max = std::max(y1, y2);
    chassert(x_max >= range_size);
    chassert(y_max >= range_size);
    return {std::pair<UInt64, UInt64>{x_max - range_size, x_max}, std::pair<UInt64, UInt64>{y_max - range_size, y_max}};
}

/** Unpack an interval of Hilbert curve to hyperrectangles covered by it across N dimensions.
  */
template <typename F>
void hilbertIntervalToHyperrectangles2D(UInt64 first, UInt64 last, F && callback)
{
    const auto equal_bits_count = getLeadingZeroBits(last | first);
    const auto even_equal_bits_count = equal_bits_count - equal_bits_count % 2;
    segmentBinaryPartition(first, last, 64 - even_equal_bits_count, [&](HilbertDetails::Segment range)
    {
        auto interval1 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(range.begin);
        auto interval2 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(range.end);

        std::array<std::pair<UInt64, UInt64>, 2> unpacked_range = createRangeFromCorners(
            std::get<0>(interval1), std::get<1>(interval1),
            std::get<0>(interval2), std::get<1>(interval2));

        callback(unpacked_range);
    });
}
