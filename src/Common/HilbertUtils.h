#include <Core/Types.h>
#include <Common/BitHelpers.h>
#include "base/types.h"
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


template <typename F>
void segmentBinaryPartition(UInt64 start, UInt64 finish, UInt8 current_bits, F && callback)
{
    if (current_bits == 0)
        return;

    auto next_bits = current_bits - 2;
    auto history = (start >> current_bits) << current_bits;

    auto start_chunk = (start >> next_bits) & 0b11;
    auto finish_chunk = (finish >> next_bits) & 0b11;

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

    auto start_range = construct_range(start_chunk);
    if (start == start_range.begin)
    {
        callback(start_range);
    }
    else
    {
        segmentBinaryPartition(start, start_range.end, next_bits, callback);
    }

    auto finish_range = construct_range(finish_chunk);
    if (finish == finish_range.end)
    {
        callback(finish_range);
    }
    else
    {
        segmentBinaryPartition(finish_range.begin, finish, next_bits, callback);
    }
}

std::array<std::pair<UInt64, UInt64>, 2> createRangeFromCorners(UInt64 x1, UInt64 y1, UInt64 x2, UInt64 y2)
{
    UInt64 dist_x = x1 > x2 ? x1 - x2 : x2 - x1;
    UInt64 dist_y = y1 > y2 ? y1 - y2 : y2 - y1;
    UInt64 range_size = std::max(dist_x, dist_y);
    UInt64 x_min = std::min(x1, x2);
    UInt64 y_min = std::min(y1, y2);
    return {
        std::pair<UInt64, UInt64>{x_min, x_min + range_size},
        std::pair<UInt64, UInt64>{y_min, y_min + range_size}
    };
}

/** Unpack an interval of Hilbert curve to hyperrectangles covered by it across N dimensions.
  */
template <typename F>
void hilbertIntervalToHyperrectangles2D(UInt64 first, UInt64 last, F && callback)
{
    segmentBinaryPartition(first, last, 64, [&](HilbertDetails::Segment range)
    {

        auto interval1 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<2>::decode(range.begin);
        auto interval2 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<2>::decode(range.end);

        std::array<std::pair<UInt64, UInt64>, 2> unpacked_range = createRangeFromCorners(
            std::get<0>(interval1), std::get<1>(interval1),
            std::get<0>(interval2), std::get<1>(interval2));

        callback(unpacked_range);
    });
}
