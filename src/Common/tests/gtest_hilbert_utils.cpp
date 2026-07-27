#include <gtest/gtest.h>

#include <vector>

#include <base/types.h>
#include <Common/HilbertUtils.h>


/// The bit-shift in `segmentBinaryPartition` at `HilbertUtils.h:78` used the
/// literal `1` which is a 32-bit `int`. When `next_bits >= 32`, the shift
/// `1 << next_bits` invoked undefined behaviour (caught by UBSan as
/// "shift exponent N is too large for 32-bit type 'int'").
///
/// The entry below deliberately drives `segmentBinaryPartition` with
/// `first` and `last` that land in the same top-level quadrant, so the
/// `start_chunk == finish_chunk` branch is taken and the problematic
/// comparison `(finish - start + 1) == (1 << next_bits)` is evaluated
/// with `next_bits = 62`. Before the fix this fires UBSan; after the fix
/// it completes normally.
TEST(HilbertUtils, SegmentBinaryPartitionNoShiftOverflow)
{
    /// Whole top quadrant: [0xC000_0000_0000_0000, 0xFFFF_FFFF_FFFF_FFFF].
    /// getLeadingZeroBits(last | first) == 0 → current_bits == 64,
    /// next_bits == 62, and both ends live in chunk 3.
    const UInt64 first = 0xC000000000000000ULL;
    const UInt64 last = 0xFFFFFFFFFFFFFFFFULL;

    std::vector<std::pair<UInt64, UInt64>> x_ranges;
    std::vector<std::pair<UInt64, UInt64>> y_ranges;

    hilbertIntervalToHyperrectangles2D(first, last, [&](std::array<std::pair<UInt64, UInt64>, 2> range)
    {
        x_ranges.push_back(range[0]);
        y_ranges.push_back(range[1]);
    });

    /// The full quadrant must produce at least one hyperrectangle.
    ASSERT_FALSE(x_ranges.empty());
    ASSERT_EQ(x_ranges.size(), y_ranges.size());
}

/// A narrower range that still exercises a high `next_bits` during recursion.
/// `first = 0x4000…0` and `last = 0x7FFF…F` covers quadrant 1, again forcing
/// `start_chunk == finish_chunk` at `next_bits == 62`.
TEST(HilbertUtils, SegmentBinaryPartitionSecondQuadrant)
{
    const UInt64 first = 0x4000000000000000ULL;
    const UInt64 last = 0x7FFFFFFFFFFFFFFFULL;

    size_t callback_count = 0;
    hilbertIntervalToHyperrectangles2D(first, last, [&](std::array<std::pair<UInt64, UInt64>, 2>)
    {
        ++callback_count;
    });

    ASSERT_GT(callback_count, 0u);
}

/// Small ranges already worked before the fix, but this guards against a
/// regression that would break short queries.
TEST(HilbertUtils, SegmentBinaryPartitionSmallRangeStillWorks)
{
    size_t callback_count = 0;
    hilbertIntervalToHyperrectangles2D(0, 15, [&](std::array<std::pair<UInt64, UInt64>, 2>)
    {
        ++callback_count;
    });

    ASSERT_GT(callback_count, 0u);
}
