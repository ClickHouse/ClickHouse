#include <Storages/ObjectStorage/DataLakes/DeletionVectorBitmap.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>

#include <limits>
#include <random>
#include <vector>


namespace
{

using Bitmap = DB::DeletionVectorBitmap;
/// The type `DeletionVectorBitmap` replaced. The cluster function protocol carries deletion
/// vectors between nodes that may run either version, so the two must serialize identically.
using LegacyBitmap = DB::RoaringBitmapWithSmallSet<size_t, 32>;

std::vector<UInt64> collect(const Bitmap & bitmap, UInt64 range_begin, UInt64 range_end)
{
    std::vector<UInt64> result;
    bitmap.forEachInRange(range_begin, range_end, [&](UInt64 value) { result.push_back(value); });
    return result;
}

/// Reference implementation: ask about every row number of the range, one by one.
std::vector<UInt64> collectByContains(const Bitmap & bitmap, UInt64 range_begin, UInt64 range_end)
{
    std::vector<UInt64> result;
    for (UInt64 value = range_begin; value < range_end; ++value)
        if (bitmap.contains(value))
            result.push_back(value);
    return result;
}

String serialize(const Bitmap & bitmap)
{
    String out;
    DB::WriteBufferFromString buffer(out);
    bitmap.write(buffer);
    buffer.finalize();
    return out;
}

String serialize(const LegacyBitmap & bitmap)
{
    String out;
    DB::WriteBufferFromString buffer(out);
    bitmap.write(buffer);
    buffer.finalize();
    return out;
}

/// Serializes the same values through both implementations, requires the bytes to be identical,
/// and reads each side's bytes back with the other implementation.
void checkSerializationMatchesLegacy(const std::vector<UInt64> & values, const String & what)
{
    Bitmap bitmap;
    LegacyBitmap legacy;
    for (UInt64 value : values)
    {
        bitmap.add(value);
        legacy.add(value);
    }

    const String bytes = serialize(bitmap);
    const String legacy_bytes = serialize(legacy);
    ASSERT_EQ(bytes, legacy_bytes) << what;

    {
        Bitmap restored;
        DB::ReadBufferFromString buffer(legacy_bytes);
        restored.read(buffer);
        ASSERT_EQ(restored.size(), values.size()) << what;
        for (UInt64 value : values)
            ASSERT_TRUE(restored.contains(value)) << what << ", value = " << value;
    }

    {
        LegacyBitmap restored;
        DB::ReadBufferFromString buffer(bytes);
        restored.read(buffer);
        ASSERT_EQ(restored.size(), values.size()) << what;
        for (UInt64 value : values)
            ASSERT_TRUE(restored.rb_contains(value)) << what << ", value = " << value;
    }
}

std::vector<UInt64> range(UInt64 begin, UInt64 end, UInt64 stride)
{
    std::vector<UInt64> values;
    for (UInt64 value = begin; value < end; value += stride)
        values.push_back(value);
    return values;
}

}

TEST(DeletionVectorBitmap, SmallSet)
{
    Bitmap bitmap;
    /// Descending, because the small representation does not keep any order.
    for (UInt64 value : {50, 40, 30, 20, 10})
        bitmap.add(value);

    EXPECT_EQ(collect(bitmap, 0, 100), (std::vector<UInt64>{10, 20, 30, 40, 50}));

    /// `range_begin` is inclusive, `range_end` is not.
    EXPECT_EQ(collect(bitmap, 20, 41), (std::vector<UInt64>{20, 30, 40}));
    EXPECT_EQ(collect(bitmap, 21, 40), (std::vector<UInt64>{30}));

    EXPECT_TRUE(collect(bitmap, 0, 10).empty());
    EXPECT_TRUE(collect(bitmap, 51, 1000).empty());
    EXPECT_TRUE(collect(bitmap, 21, 30).empty());
    EXPECT_TRUE(collect(bitmap, 30, 30).empty());
    EXPECT_TRUE(collect(bitmap, 40, 20).empty());
}

TEST(DeletionVectorBitmap, LargeSet)
{
    Bitmap bitmap;
    for (UInt64 i = 0; i < 100; ++i)
        bitmap.add(i * 7);

    EXPECT_EQ(collect(bitmap, 0, 700).size(), 100u);

    /// Seeking into the middle of the set must not return anything before the range.
    EXPECT_EQ(collect(bitmap, 350, 372), (std::vector<UInt64>{350, 357, 364, 371}));
    EXPECT_EQ(collect(bitmap, 351, 371), (std::vector<UInt64>{357, 364}));

    EXPECT_TRUE(collect(bitmap, 351, 357).empty());
    EXPECT_TRUE(collect(bitmap, 694, 700).empty());
    EXPECT_TRUE(collect(bitmap, 700, 100000).empty());
    EXPECT_TRUE(collect(bitmap, 350, 350).empty());
}

TEST(DeletionVectorBitmap, MatchesContains)
{
    /// Values spread far apart, crossing both the 16-bit container boundaries of a roaring bitmap
    /// and the 32-bit boundary at which a new inner bitmap starts.
    const std::vector<UInt64> values{
        0, 1, 2, 4095, 4096, 65535, 65536, 65537, 131071, 1048576,
        4294967295, 4294967296, 4294967297, 8589934591, 8589934592};

    for (bool large : {false, true})
    {
        Bitmap bitmap;
        for (UInt64 value : values)
            bitmap.add(value);
        /// Adding more than `small_set_size` values switches to the roaring representation.
        if (large)
            for (UInt64 i = 0; i < 100; ++i)
                bitmap.add(2000000 + i);

        for (UInt64 begin : {UInt64(0), UInt64(1), UInt64(4090), UInt64(65530), UInt64(131060),
                             UInt64(4294967290), UInt64(4294967296), UInt64(8589934580)})
        {
            const UInt64 end = begin + 16;
            EXPECT_EQ(collect(bitmap, begin, end), collectByContains(bitmap, begin, end))
                << "large = " << large << ", begin = " << begin;
        }
    }
}

/// The roaring values are read in batches, so the range may end in the middle of a batch, exactly
/// on a batch boundary, or after the last one.
TEST(DeletionVectorBitmap, BatchBoundaries)
{
    Bitmap bitmap;
    for (UInt64 i = 0; i < 2000; ++i)
        bitmap.add(i);

    for (UInt64 end : {UInt64(1), UInt64(255), UInt64(256), UInt64(257), UInt64(511), UInt64(512),
                       UInt64(513), UInt64(1024), UInt64(1999), UInt64(2000), UInt64(2001)})
    {
        const std::vector<UInt64> expected = range(0, std::min<UInt64>(end, 2000), 1);
        EXPECT_EQ(collect(bitmap, 0, end), expected) << "end = " << end;
    }

    /// The same, but starting in the middle so that the first batch is not aligned with the range.
    for (UInt64 begin : {UInt64(1), UInt64(255), UInt64(256), UInt64(257)})
    {
        const UInt64 end = begin + 600;
        EXPECT_EQ(collect(bitmap, begin, end), range(begin, end, 1)) << "begin = " << begin;
    }
}

TEST(DeletionVectorBitmap, SparseAndDenseMatchContains)
{
    std::mt19937_64 rng(20260805);
    for (UInt64 stride : {UInt64(1), UInt64(2), UInt64(3), UInt64(97)})
    {
        Bitmap bitmap;
        for (UInt64 value = 0; value < 5000; value += stride)
            bitmap.add(value);

        for (int attempt = 0; attempt < 50; ++attempt)
        {
            const UInt64 begin = rng() % 5000;
            const UInt64 end = begin + rng() % 800;
            EXPECT_EQ(collect(bitmap, begin, end), collectByContains(bitmap, begin, end))
                << "stride = " << stride << ", begin = " << begin << ", end = " << end;
        }
    }
}

TEST(DeletionVectorBitmap, Size)
{
    Bitmap bitmap;
    EXPECT_EQ(bitmap.size(), 0u);

    for (UInt64 i = 0; i < 10; ++i)
        bitmap.add(i);
    EXPECT_EQ(bitmap.size(), 10u);

    /// Duplicates must not be counted twice, in either representation.
    for (UInt64 i = 0; i < 10; ++i)
        bitmap.add(i);
    EXPECT_EQ(bitmap.size(), 10u);

    for (UInt64 i = 0; i < 1000; ++i)
        bitmap.add(i);
    EXPECT_EQ(bitmap.size(), 1000u);

    for (UInt64 i = 0; i < 1000; ++i)
        bitmap.add(i);
    EXPECT_EQ(bitmap.size(), 1000u);
}

TEST(DeletionVectorBitmap, SerializationMatchesLegacy)
{
    checkSerializationMatchesLegacy({}, "empty");
    checkSerializationMatchesLegacy({0}, "single zero");
    checkSerializationMatchesLegacy(range(0, 32, 1), "full small set");
    /// One past the point where the small representation is abandoned.
    checkSerializationMatchesLegacy(range(0, 33, 1), "just switched to roaring");
    checkSerializationMatchesLegacy(range(0, 4096, 512), "sparse, one array container");
    checkSerializationMatchesLegacy(range(0, 65536, 2), "dense, one bitset container");
    /// Consecutive values collapse into runs, which `write` compresses with `runOptimize`.
    checkSerializationMatchesLegacy(range(0, 65536, 1), "contiguous, one run container");
    checkSerializationMatchesLegacy(range(0, 200000, 3), "several containers");
    checkSerializationMatchesLegacy(
        {0, 1, 4294967295, 4294967296, 4294967297, 8589934592, 18446744073709551615ULL}, "across the 32-bit boundary");

    std::mt19937_64 rng(20260805);
    std::vector<UInt64> random_values;
    for (int i = 0; i < 20000; ++i)
        random_values.push_back(rng() % (1ULL << 40));
    checkSerializationMatchesLegacy(random_values, "random over 2^40");
}

/// Insertion caches the leaf the previous value landed in, and that leaf points into the bitmap.
/// Anything that replaces the bitmap has to drop the cache, otherwise the next insertion follows a
/// dangling pointer. These are the paths that replace it.
TEST(DeletionVectorBitmap, InsertionAfterBitmapIsReplaced)
{
    /// Reading into a bitmap that already holds the roaring representation.
    {
        Bitmap bitmap;
        for (UInt64 i = 0; i < 1000; ++i)
            bitmap.add(i);

        Bitmap other;
        for (UInt64 i = 5000; i < 6000; ++i)
            other.add(i);
        const String bytes = serialize(other);

        DB::ReadBufferFromString buffer(bytes);
        bitmap.read(buffer);

        for (UInt64 i = 7000; i < 8000; ++i)
            bitmap.add(i);

        EXPECT_EQ(bitmap.size(), 2000u);
        EXPECT_EQ(collect(bitmap, 0, 20000), collectByContains(bitmap, 0, 20000));
    }

    /// Reading the small representation over the roaring one, then growing back past the threshold.
    {
        Bitmap bitmap;
        for (UInt64 i = 0; i < 1000; ++i)
            bitmap.add(i);

        Bitmap small_one;
        small_one.add(42);
        const String bytes = serialize(small_one);

        DB::ReadBufferFromString buffer(bytes);
        bitmap.read(buffer);
        EXPECT_EQ(bitmap.size(), 1u);

        for (UInt64 i = 100; i < 200; ++i)
            bitmap.add(i);

        EXPECT_EQ(bitmap.size(), 101u);
        EXPECT_EQ(collect(bitmap, 0, 1000), collectByContains(bitmap, 0, 1000));
    }

    /// Crossing the small-set threshold, which builds the roaring representation from scratch.
    {
        Bitmap bitmap;
        for (UInt64 i = 0; i < 200; ++i)
        {
            bitmap.add(i);
            ASSERT_EQ(bitmap.size(), i + 1) << "i = " << i;
        }
        EXPECT_EQ(collect(bitmap, 0, 200), collectByContains(bitmap, 0, 200));
    }
}

TEST(DeletionVectorBitmap, ReadRejectsMalformedInput)
{
    const auto read_from = [](const String & bytes)
    {
        Bitmap bitmap;
        DB::ReadBufferFromString buffer(bytes);
        bitmap.read(buffer);
    };

    {
        String bytes;
        DB::WriteBufferFromString out(bytes);
        DB::writeBinary(static_cast<UInt8>(7), out);
        out.finalize();
        EXPECT_THROW(read_from(bytes), DB::Exception) << "unknown kind";
    }

    {
        String bytes;
        DB::WriteBufferFromString out(bytes);
        DB::writeBinary(static_cast<UInt8>(1), out);
        DB::writeVarUInt(0, out);
        out.finalize();
        EXPECT_THROW(read_from(bytes), DB::Exception) << "zero size";
    }

    {
        String bytes;
        DB::WriteBufferFromString out(bytes);
        DB::writeBinary(static_cast<UInt8>(1), out);
        DB::writeVarUInt(1024ULL * 1024 * 1024 * 1024, out);
        out.finalize();
        EXPECT_THROW(read_from(bytes), DB::Exception) << "size beyond the limit";
    }

    {
        String bytes;
        DB::WriteBufferFromString out(bytes);
        DB::writeBinary(static_cast<UInt8>(1), out);
        DB::writeVarUInt(16, out);
        out.write(String(16, '\xFF').data(), 16);
        out.finalize();
        EXPECT_THROW(read_from(bytes), DB::Exception) << "garbage where a bitmap is expected";
    }
}

TEST(DeletionVectorBitmap, UpperBoundOfTheValueRange)
{
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();

    Bitmap bitmap;
    for (UInt64 value : range(max - 100, max, 1))
        bitmap.add(value);
    bitmap.add(max);
    ASSERT_EQ(bitmap.size(), 101u);

    /// `range_end` is a `UInt64` and the range is half-open, so the very last row number can never
    /// fall inside it. Everything below it still has to come out, and the scan has to terminate.
    const std::vector<UInt64> below_max = range(max - 100, max, 1);
    ASSERT_EQ(below_max.size(), 100u);
    EXPECT_EQ(collect(bitmap, max - 100, max), below_max);
    EXPECT_EQ(collect(bitmap, 0, max), below_max);

    EXPECT_TRUE(bitmap.contains(max));
    EXPECT_TRUE(collect(bitmap, max, max).empty());
}

TEST(DeletionVectorBitmap, Empty)
{
    Bitmap bitmap;
    EXPECT_EQ(bitmap.size(), 0u);
    EXPECT_FALSE(bitmap.contains(0));
    EXPECT_TRUE(collect(bitmap, 0, 1000).empty());
    EXPECT_TRUE(collect(bitmap, 0, std::numeric_limits<UInt64>::max()).empty());
}
