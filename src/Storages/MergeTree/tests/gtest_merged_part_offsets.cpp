#include <random>
#include <gtest/gtest.h>

#include <Storages/MergeTree/MergedPartOffsets.h>

using namespace DB;

// PAGE_SIZE is 1024 (2^10)
constexpr static size_t PAGE_SIZE = 1024;

// Helper to generate monotonically increasing values
std::vector<UInt64> generateMonotonicValues(size_t count, UInt64 start = 0, UInt64 max_step = 10)
{
    std::vector<UInt64> result;
    result.reserve(count);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<UInt64> dist(1, max_step);

    UInt64 current = start;
    for (size_t i = 0; i < count; ++i)
    {
        result.push_back(current);
        current += dist(gen);
    }

    return result;
}

//////////////////////////
// PackedPartOffsets Tests
//////////////////////////

// Basic functionality test
TEST(PackedPartOffsetsTest, BasicInsertAndRetrieve)
{
    PackedPartOffsets offsets;

    // Insert some values
    offsets.insert(1);
    offsets.insert(2);
    offsets.insert(5);
    offsets.insert(10);
    offsets.insert(20);

    // Flush to ensure values are compressed
    offsets.flush();

    // Verify retrieval
    EXPECT_EQ(offsets[0], 1);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 5);
    EXPECT_EQ(offsets[3], 10);
    EXPECT_EQ(offsets[4], 20);
}

// Single value page test
TEST(PackedPartOffsetsTest, SingleValuePage)
{
    PackedPartOffsets offsets;

    offsets.insert(1);
    offsets.flush();
    EXPECT_EQ(offsets[0], 1);
}

// Test with exactly one page of data
TEST(PackedPartOffsetsTest, ExactlyOnePage)
{
    PackedPartOffsets offsets;
    auto values = generateMonotonicValues(PAGE_SIZE);

    // Insert all values
    for (const auto & val : values)
    {
        offsets.insert(val);
    }

    // Flush to ensure values are compressed
    offsets.flush();

    // Verify all values were stored correctly
    for (size_t i = 0; i < PAGE_SIZE; ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with multiple pages
TEST(PackedPartOffsetsTest, MultiplePages)
{
    PackedPartOffsets offsets;
    const size_t num_pages = 3;
    auto values = generateMonotonicValues(PAGE_SIZE * num_pages);

    // Insert all values
    for (const auto & val : values)
    {
        offsets.insert(val);
    }

    // Flush to ensure values are compressed
    offsets.flush();

    // Verify all values were stored correctly
    for (size_t i = 0; i < PAGE_SIZE * num_pages; ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test auto-flush when page fills up
TEST(PackedPartOffsetsTest, AutoFlushOnPageFill)
{
    PackedPartOffsets offsets;
    const size_t total_values = PAGE_SIZE + 100;
    auto values = generateMonotonicValues(total_values);

    // Insert all values (should auto-flush when page fills)
    for (const auto & val : values)
    {
        offsets.insert(val);
    }

    // Final flush to compress any remaining values
    offsets.flush();

    // Verify all values were stored correctly
    for (size_t i = 0; i < total_values; ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with minimal bit width (1 bit)
TEST(PackedPartOffsetsTest, MinimalBitWidth)
{
    PackedPartOffsets offsets;

    // Create values that only need 1 bit to represent differences
    std::vector<UInt64> values = {100, 101, 102, 103, 104, 105};

    // Insert and flush
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // Verify
    for (size_t i = 0; i < values.size(); ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with maximum bit width (64 bits)
TEST(PackedPartOffsetsTest, MaximumBitWidth)
{
    PackedPartOffsets offsets;

    // Values that require full 64 bits
    UInt64 base = 1000;
    std::vector<UInt64> values = {base, base + (1ULL << 62), base + (1ULL << 63)};

    // Insert and flush
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // Verify
    for (size_t i = 0; i < values.size(); ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with values that span word boundaries during bit packing
TEST(PackedPartOffsetsTest, ValueSpanningWordBoundaries)
{
    PackedPartOffsets offsets;

    // Create values where differences require multi-word storage
    // Choose values where differences will be around 60-bits wide
    // and ensure they would cross 64-bit word boundaries
    UInt64 base = 1000;
    std::vector<UInt64> values;
    values.push_back(base);

    for (size_t i = 1; i < 20; ++i)
    {
        // Add values that ensure bit-packing will cross word boundaries
        values.push_back(values.back() + (1ULL << 59) + i);
    }

    // Insert and flush
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // Verify
    for (size_t i = 0; i < values.size(); ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with large gaps between values
TEST(PackedPartOffsetsTest, LargeValueGaps)
{
    PackedPartOffsets offsets;

    // Values with very large gaps
    std::vector<UInt64> values = {1ULL, 1000000ULL, 1000000000ULL, 1000000000000ULL, 1ULL << 40, 1ULL << 50};

    // Insert and flush
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // Verify
    for (size_t i = 0; i < values.size(); ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with very small values
TEST(PackedPartOffsetsTest, VerySmallValues)
{
    PackedPartOffsets offsets;

    // Small consecutive values
    std::vector<UInt64> values = {0, 1, 2, 3, 4, 5};

    // Insert and flush
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // Verify
    for (size_t i = 0; i < values.size(); ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with very large values
TEST(PackedPartOffsetsTest, VeryLargeValues)
{
    PackedPartOffsets offsets;

    // Large values close to UInt64 max
    std::vector<UInt64> values
        = {std::numeric_limits<UInt64>::max() - 1000,
           std::numeric_limits<UInt64>::max() - 800,
           std::numeric_limits<UInt64>::max() - 600,
           std::numeric_limits<UInt64>::max() - 400,
           std::numeric_limits<UInt64>::max() - 200,
           std::numeric_limits<UInt64>::max() - 1};

    // Insert and flush
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // Verify
    for (size_t i = 0; i < values.size(); ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with all identical values (should fail assertion)
TEST(PackedPartOffsetsTest, DISABLED_AllIdenticalValues)
{
    // Note: This test is disabled because it should trigger an assertion failure
    // Enable with caution for debugging
    PackedPartOffsets offsets;

    offsets.insert(100);
    offsets.insert(100); // This should trigger assertion
}

// Test with non-monotonic values (should fail assertion)
TEST(PackedPartOffsetsTest, DISABLED_NonMonotonicValues)
{
    // Note: This test is disabled because it should trigger an assertion failure
    // Enable with caution for debugging
    PackedPartOffsets offsets;

    offsets.insert(100);
    offsets.insert(200);
    offsets.insert(150); // This should trigger assertion
}

// Test memory allocation
TEST(PackedPartOffsetsTest, MemoryAllocation)
{
    PackedPartOffsets offsets;

    // Empty structure should have minimal memory allocation
    size_t empty_memory = offsets.totalAllocatedMemory();

    // Insert some values
    constexpr static size_t NUM_VALUES = 1000;
    auto values = generateMonotonicValues(NUM_VALUES);
    for (const auto & val : values)
    {
        offsets.insert(val);
    }
    offsets.flush();

    // After insertion, memory usage should increase
    size_t filled_memory = offsets.totalAllocatedMemory();
    EXPECT_GT(filled_memory, empty_memory);
}

//////////////////////////
// MergedPartOffsets Tests
//////////////////////////

TEST(MergedPartOffsetsTest, SinglePart)
{
    std::vector<UInt64> part_indices(4);
    MergedPartOffsets merged_offsets(1);
    merged_offsets.insert(part_indices.data(), part_indices.data() + part_indices.size());
    merged_offsets.flush();

    EXPECT_EQ((merged_offsets[0, 0]), 0);
    EXPECT_EQ((merged_offsets[0, 1]), 1);
    EXPECT_EQ((merged_offsets[0, 2]), 2);
    EXPECT_EQ((merged_offsets[0, 3]), 3);
}

TEST(MergedPartOffsetsTest, MultipleParts)
{
    std::vector<UInt64> part_indices = {0, 1, 2, 0, 1, 2, 0, 1, 2};
    MergedPartOffsets merged_offsets(3);
    merged_offsets.insert(part_indices.data(), part_indices.data() + part_indices.size());
    merged_offsets.flush();

    EXPECT_EQ((merged_offsets[0, 0]), 0);
    EXPECT_EQ((merged_offsets[1, 0]), 1);
    EXPECT_EQ((merged_offsets[2, 0]), 2);
    EXPECT_EQ((merged_offsets[0, 1]), 3);
    EXPECT_EQ((merged_offsets[1, 1]), 4);
    EXPECT_EQ((merged_offsets[2, 1]), 5);
    EXPECT_EQ((merged_offsets[0, 2]), 6);
    EXPECT_EQ((merged_offsets[1, 2]), 7);
    EXPECT_EQ((merged_offsets[2, 2]), 8);
}

// Test size() and empty() methods
TEST(MergedPartOffsetsTest, SizeAndEmpty)
{
    // Setup merged_offsets
    MergedPartOffsets merged_offsets(2);

    // Initially empty
    EXPECT_TRUE(merged_offsets.empty());
    EXPECT_EQ(merged_offsets.size(), 0);

    // Insert some offsets
    std::vector<UInt64> part_indices = {0, 0, 1};
    merged_offsets.insert(part_indices.data(), part_indices.data() + part_indices.size());

    // Not empty now
    EXPECT_FALSE(merged_offsets.empty());
    EXPECT_EQ(merged_offsets.size(), 3);

    // Insert more
    std::vector<UInt64> more_part_indices = {1, 0};
    merged_offsets.insert(more_part_indices.data(), more_part_indices.data() + more_part_indices.size());

    // Size should increase
    EXPECT_EQ(merged_offsets.size(), 5);

    // Flush should not change size
    merged_offsets.flush();
    EXPECT_EQ(merged_offsets.size(), 5);
}

TEST(MergedPartOffsetsTest, ManyValues)
{
    MergedPartOffsets merged_offsets(3);
    std::vector<UInt64> part_indices;

    part_indices.reserve(15000);

    for (int i = 0; i < 5000; ++i)
        part_indices.push_back(0);
    for (int i = 0; i < 5000; ++i)
        part_indices.push_back(1);
    for (int i = 0; i < 5000; ++i)
        part_indices.push_back(2);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(part_indices.begin(), part_indices.end(), g);

    merged_offsets.insert(part_indices.data(), part_indices.data() + part_indices.size());
    merged_offsets.flush();

    EXPECT_EQ(merged_offsets.size(), 15000);

    std::vector<UInt64> offsets(3);
    for (size_t i = 0; i < 15000; ++i)
    {
        auto part = part_indices[i];
        EXPECT_EQ((merged_offsets[part, offsets[part]]), i);
        ++offsets[part];
    }
}
