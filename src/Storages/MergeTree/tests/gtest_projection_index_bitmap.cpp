#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

using namespace DB;

// Test basic creation of 32-bit bitmap
TEST(ProjectionIndexBitmapTest, Create32)
{
    auto bitmap = ProjectionIndexBitmap::create32();
    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap->type, ProjectionIndexBitmap::BitmapType::Bitmap32);
    EXPECT_TRUE(bitmap->empty());
    EXPECT_EQ(bitmap->cardinality(), 0);
}

// Test basic creation of 64-bit bitmap
TEST(ProjectionIndexBitmapTest, Create64)
{
    auto bitmap = ProjectionIndexBitmap::create64();
    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap->type, ProjectionIndexBitmap::BitmapType::Bitmap64);
    EXPECT_TRUE(bitmap->empty());
    EXPECT_EQ(bitmap->cardinality(), 0);
}

// Test add and contains for 32-bit bitmap
TEST(ProjectionIndexBitmapTest, AddContains32)
{
    auto bitmap = ProjectionIndexBitmap::create32();
    ASSERT_NE(bitmap, nullptr);

    // Add values
    bitmap->add<UInt32>(10);
    bitmap->add<UInt32>(20);
    bitmap->add<UInt32>(30);

    // Check cardinality and emptiness
    EXPECT_FALSE(bitmap->empty());
    EXPECT_EQ(bitmap->cardinality(), 3);

    // Check contains
    EXPECT_TRUE(bitmap->contains<UInt32>(10));
    EXPECT_TRUE(bitmap->contains<UInt32>(20));
    EXPECT_TRUE(bitmap->contains<UInt32>(30));
    EXPECT_FALSE(bitmap->contains<UInt32>(15));
    EXPECT_FALSE(bitmap->contains<UInt32>(0));
}

// Test add and contains for 64-bit bitmap
TEST(ProjectionIndexBitmapTest, AddContains64)
{
    auto bitmap = ProjectionIndexBitmap::create64();
    ASSERT_NE(bitmap, nullptr);

    // Add large values that need 64 bits
    UInt64 val1 = 400000000000ULL;
    UInt64 val2 = 500000000000ULL;
    UInt64 val3 = 600000000000ULL;

    bitmap->add<UInt64>(val1);
    bitmap->add<UInt64>(val2);
    bitmap->add<UInt64>(val3);

    // Check cardinality and emptiness
    EXPECT_FALSE(bitmap->empty());
    EXPECT_EQ(bitmap->cardinality(), 3);

    // Check contains
    EXPECT_TRUE(bitmap->contains<UInt64>(val1));
    EXPECT_TRUE(bitmap->contains<UInt64>(val2));
    EXPECT_TRUE(bitmap->contains<UInt64>(val3));
    EXPECT_FALSE(bitmap->contains<UInt64>(val1 + 1));
    EXPECT_FALSE(bitmap->contains<UInt64>(0));
}

// Test intersection of 32-bit bitmaps
TEST(ProjectionIndexBitmapTest, Intersection32)
{
    auto bitmap1 = ProjectionIndexBitmap::create32();
    auto bitmap2 = ProjectionIndexBitmap::create32();

    // Add values to bitmap1: {10, 20, 30, 40}
    bitmap1->add<UInt32>(10);
    bitmap1->add<UInt32>(20);
    bitmap1->add<UInt32>(30);
    bitmap1->add<UInt32>(40);

    // Add values to bitmap2: {20, 30, 50, 60}
    bitmap2->add<UInt32>(20);
    bitmap2->add<UInt32>(30);
    bitmap2->add<UInt32>(50);
    bitmap2->add<UInt32>(60);

    // Expected intersection: {20, 30}
    bitmap1->intersectWith(*bitmap2);

    // Check result
    EXPECT_EQ(bitmap1->cardinality(), 2);
    EXPECT_FALSE(bitmap1->contains<UInt32>(10));
    EXPECT_TRUE(bitmap1->contains<UInt32>(20));
    EXPECT_TRUE(bitmap1->contains<UInt32>(30));
    EXPECT_FALSE(bitmap1->contains<UInt32>(40));
    EXPECT_FALSE(bitmap1->contains<UInt32>(50));
    EXPECT_FALSE(bitmap1->contains<UInt32>(60));
}

// Test intersection of 64-bit bitmaps
TEST(ProjectionIndexBitmapTest, Intersection64)
{
    auto bitmap1 = ProjectionIndexBitmap::create64();
    auto bitmap2 = ProjectionIndexBitmap::create64();

    // Add values to bitmap1
    UInt64 val1 = 4000000000000ULL;
    UInt64 val2 = 5000000000000ULL;
    UInt64 val3 = 6000000000000ULL;
    UInt64 val4 = 7000000000000ULL;

    bitmap1->add<UInt64>(val1);
    bitmap1->add<UInt64>(val2);
    bitmap1->add<UInt64>(val3);
    bitmap1->add<UInt64>(val4);

    // Add values to bitmap2
    bitmap2->add<UInt64>(val2);
    bitmap2->add<UInt64>(val3);
    bitmap2->add<UInt64>(800000000000ULL);
    bitmap2->add<UInt64>(900000000000ULL);

    // Expected intersection: {val2, val3}
    bitmap1->intersectWith(*bitmap2);

    // Check result
    EXPECT_EQ(bitmap1->cardinality(), 2);
    EXPECT_FALSE(bitmap1->contains<UInt64>(val1));
    EXPECT_TRUE(bitmap1->contains<UInt64>(val2));
    EXPECT_TRUE(bitmap1->contains<UInt64>(val3));
    EXPECT_FALSE(bitmap1->contains<UInt64>(val4));
}

// Test rangeAllZero on 32-bit bitmap
TEST(ProjectionIndexBitmapTest, RangeAllZero32)
{
    auto bitmap = ProjectionIndexBitmap::create32();

    // Add values at specific positions
    bitmap->add<UInt32>(101);
    bitmap->add<UInt32>(103);
    bitmap->add<UInt32>(107);

    // Check ranges with and without values
    EXPECT_TRUE(bitmap->rangeAllZero(100, 101)) << "Expected [100,101) to be zero";
    EXPECT_FALSE(bitmap->rangeAllZero(100, 102)) << "Expected [100,102) to be non-zero";
    EXPECT_TRUE(bitmap->rangeAllZero(102, 103)) << "Expected [102,103) to be zero";
    EXPECT_FALSE(bitmap->rangeAllZero(103, 108)) << "Expected [103,108) to be non-zero";
    EXPECT_TRUE(bitmap->rangeAllZero(108, 110)) << "Expected [108,110) to be zero";
}

// Test rangeAllZero on 64-bit bitmap
TEST(ProjectionIndexBitmapTest, RangeAllZero64)
{
    auto bitmap = ProjectionIndexBitmap::create64();

    UInt64 base = 1'000'000'000'000ULL;
    bitmap->add<UInt64>(base + 2);
    bitmap->add<UInt64>(base + 4);
    bitmap->add<UInt64>(base + 6);

    EXPECT_TRUE(bitmap->rangeAllZero(base, base + 2)) << "Expected [base, base+2) to be zero";
    EXPECT_FALSE(bitmap->rangeAllZero(base, base + 3)) << "Expected [base, base+3) to be non-zero";
    EXPECT_TRUE(bitmap->rangeAllZero(base + 3, base + 4)) << "Expected [base+3, base+4) to be zero";
    EXPECT_FALSE(bitmap->rangeAllZero(base + 4, base + 7)) << "Expected [base+4, base+7) to be non-zero";
    EXPECT_TRUE(bitmap->rangeAllZero(base + 7, base + 10)) << "Expected [base+7, base+10) to be zero";
}

// Test rangeAllZero on completely empty bitmap
TEST(ProjectionIndexBitmapTest, RangeAllZeroEmpty)
{
    auto bitmap = ProjectionIndexBitmap::create32();

    EXPECT_TRUE(bitmap->rangeAllZero(0, 10)) << "Expected empty bitmap range to be zero";
    EXPECT_TRUE(bitmap->rangeAllZero(1000, 2000)) << "Expected empty bitmap large range to be zero";
}

// Test rangeAllZero with begin == end (empty range)
TEST(ProjectionIndexBitmapTest, RangeAllZeroEmptyRange)
{
    auto bitmap = ProjectionIndexBitmap::create32();
    bitmap->add<UInt32>(123);

    EXPECT_TRUE(bitmap->rangeAllZero(100, 100)) << "Empty range should always be considered all zero";
    EXPECT_TRUE(bitmap->rangeAllZero(123, 123)) << "Even on a value, empty range should return true";
}

// Test appendToFilter for 32-bit bitmap
TEST(ProjectionIndexBitmapTest, AppendToFilter32)
{
    auto bitmap = ProjectionIndexBitmap::create32();

    // Add values {101, 103, 107}
    bitmap->add<UInt32>(101);
    bitmap->add<UInt32>(103);
    bitmap->add<UInt32>(107);

    // Define range [100, 110)
    UInt32 start = 100;
    size_t size = 10;

    // Initialize buffer with some values
    PaddedPODArray<UInt8> buffer(5, 0);

    // Append to filter
    bool has_values = bitmap->appendToFilter(buffer, start, size);

    // Check result
    EXPECT_TRUE(has_values);

    std::vector<UInt8> expected = {0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0};
    for (size_t i = 0; i < size; ++i) {
        EXPECT_EQ(buffer[i], expected[i]) << "Mismatch at position " << i;
    }
}

// Test appendToFilter for 64-bit bitmap with values in range
TEST(ProjectionIndexBitmapTest, AppendToFilter64WithValues)
{
    auto bitmap = ProjectionIndexBitmap::create64();

    // Add large values
    UInt64 base = 10000000000000ULL;
    bitmap->add<UInt64>(base + 1);
    bitmap->add<UInt64>(base + 3);
    bitmap->add<UInt64>(base + 7);

    // Define range
    UInt64 start = base;
    size_t size = 10;

    // Initialize buffer with some values;
    PaddedPODArray<UInt8> buffer(5, 0);

    // Append to filter
    bool has_values = bitmap->appendToFilter(buffer, start, size);

    // Check result
    EXPECT_TRUE(has_values);

    std::vector<UInt8> expected = {0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0};
    for (size_t i = 0; i < size; ++i) {
        EXPECT_EQ(buffer[i], expected[i]) << "Mismatch at position " << i;
    }
}

// Test appendToFilter with no values in range
TEST(ProjectionIndexBitmapTest, AppendToFilterNoValues)
{
    auto bitmap = ProjectionIndexBitmap::create32();

    // Add values outside the range
    bitmap->add<UInt32>(50);
    bitmap->add<UInt32>(150);

    // Define range [100, 110)
    UInt32 start = 100;
    size_t size = 10;

    PaddedPODArray<UInt8> buffer;

    // Append to filter
    bool has_values = bitmap->appendToFilter(buffer, start, size);

    // Check result
    EXPECT_FALSE(has_values);

    // Buffer should still be all zeros
    for (size_t i = 0; i < size; ++i) {
        EXPECT_EQ(buffer[i], 0) << "Buffer should remain zeroed at position " << i;
    }
}

// Test edge cases - empty intersection
TEST(ProjectionIndexBitmapTest, EmptyIntersection)
{
    auto bitmap1 = ProjectionIndexBitmap::create32();
    auto bitmap2 = ProjectionIndexBitmap::create32();

    bitmap1->add<UInt32>(10);
    bitmap1->add<UInt32>(20);

    bitmap2->add<UInt32>(30);
    bitmap2->add<UInt32>(40);

    bitmap1->intersectWith(*bitmap2);

    EXPECT_TRUE(bitmap1->empty());
    EXPECT_EQ(bitmap1->cardinality(), 0);
}

// Test adding duplicate values
TEST(ProjectionIndexBitmapTest, DuplicateValues)
{
    auto bitmap = ProjectionIndexBitmap::create32();

    bitmap->add<UInt32>(10);
    bitmap->add<UInt32>(10); // Add duplicate

    EXPECT_EQ(bitmap->cardinality(), 1); // Should still have cardinality 1
    EXPECT_TRUE(bitmap->contains<UInt32>(10));
}
