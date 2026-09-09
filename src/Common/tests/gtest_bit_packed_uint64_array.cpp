#include <gtest/gtest.h>

#include <random>

#include <Common/BitPackedUInt64Array.h>
#include <Common/Exception.h>

using namespace DB;

namespace
{

void checkRoundTrip(const std::vector<UInt64> & values)
{
    BitPackedUInt64Array packed{std::span<const UInt64>(values)};

    ASSERT_EQ(packed.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i)
        ASSERT_EQ(packed.get(i), values[i]) << "at index " << i;
}

}

TEST(BitPackedUInt64Array, Empty)
{
    BitPackedUInt64Array packed;
    EXPECT_EQ(packed.size(), 0u);
    EXPECT_TRUE(packed.empty());

    BitPackedUInt64Array packed_from_empty{std::span<const UInt64>{}};
    EXPECT_EQ(packed_from_empty.size(), 0u);
    EXPECT_TRUE(packed_from_empty.empty());
}

TEST(BitPackedUInt64Array, SingleValue)
{
    checkRoundTrip({0});
    checkRoundTrip({42});
    checkRoundTrip({std::numeric_limits<UInt64>::max()});
}

TEST(BitPackedUInt64Array, AllValuesEqual)
{
    /// Zero bits per value.
    checkRoundTrip(std::vector<UInt64>(1000, 123456789));
}

TEST(BitPackedUInt64Array, FullWidthValues)
{
    /// Differences require all 64 bits.
    checkRoundTrip({0, std::numeric_limits<UInt64>::max(), 1, std::numeric_limits<UInt64>::max() - 1});
}

TEST(BitPackedUInt64Array, BlockBoundaries)
{
    std::vector<UInt64> values;
    for (size_t size : {255, 256, 257, 512, 513})
    {
        values.clear();
        for (size_t i = 0; i < size; ++i)
            values.push_back(i * 1000);

        checkRoundTrip(values);
    }
}

TEST(BitPackedUInt64Array, IncreasingOffsets)
{
    /// Emulates offsets in a file: steadily increasing with variable steps.
    std::mt19937_64 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
    std::vector<UInt64> values;
    UInt64 current = 0;

    for (size_t i = 0; i < 10000; ++i)
    {
        current += rng() % 100000;
        values.push_back(current);
    }

    checkRoundTrip(values);
}

TEST(BitPackedUInt64Array, RandomValues)
{
    std::mt19937_64 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
    std::vector<UInt64> values;

    for (size_t i = 0; i < 10000; ++i)
        values.push_back(rng());

    checkRoundTrip(values);
}

/// Skipped under debug/sanitizers: LOGICAL_ERROR aborts there, so EXPECT_THROW can't catch it.
#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(BitPackedUInt64Array, OutOfRangeThrows)
{
    BitPackedUInt64Array empty;
    EXPECT_THROW(empty.get(0), Exception);

    std::vector<UInt64> values{1, 2, 3};
    BitPackedUInt64Array packed{std::span<const UInt64>(values)};
    EXPECT_THROW(packed.get(3), Exception);
}

#endif
