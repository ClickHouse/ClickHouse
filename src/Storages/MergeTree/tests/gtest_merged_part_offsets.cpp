#include <random>
#include <gtest/gtest.h>

#include <Storages/MergeTree/MergedPartOffsets.h>

using namespace DB;

// TEST_PAGE_SIZE is 1024 (2^10), matching MergedPartOffsets' internal page size.
// Named to avoid PAGE_SIZE, which musl's <limits.h> defines as a macro (unlike glibc).
constexpr static size_t TEST_PAGE_SIZE = 1024;

// Helper to generate monotonically increasing values
static std::vector<UInt64> generateMonotonicValues(size_t count, UInt64 start = 0, UInt64 max_step = 10)
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
    auto values = generateMonotonicValues(TEST_PAGE_SIZE);

    // Insert all values
    for (const auto & val : values)
    {
        offsets.insert(val);
    }

    // Flush to ensure values are compressed
    offsets.flush();

    // Verify all values were stored correctly
    for (size_t i = 0; i < TEST_PAGE_SIZE; ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test with multiple pages
TEST(PackedPartOffsetsTest, MultiplePages)
{
    PackedPartOffsets offsets;
    const size_t num_pages = 3;
    auto values = generateMonotonicValues(TEST_PAGE_SIZE * num_pages);

    // Insert all values
    for (const auto & val : values)
    {
        offsets.insert(val);
    }

    // Flush to ensure values are compressed
    offsets.flush();

    // Verify all values were stored correctly
    for (size_t i = 0; i < TEST_PAGE_SIZE * num_pages; ++i)
    {
        EXPECT_EQ(offsets[i], values[i]);
    }
}

// Test auto-flush when page fills up
TEST(PackedPartOffsetsTest, AutoFlushOnPageFill)
{
    PackedPartOffsets offsets;
    const size_t total_values = TEST_PAGE_SIZE + 100;
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

// Consecutive values spanning several pages: the map is the affine function first_value + i
TEST(PackedPartOffsetsTest, ConsecutiveValues)
{
    PackedPartOffsets offsets;

    constexpr UInt64 first = 12345;
    const size_t count = (3 * TEST_PAGE_SIZE) + 7;

    for (size_t i = 0; i < count; ++i)
        offsets.insert(first + i);
    offsets.flush();

    for (size_t i = 0; i < count; ++i)
        EXPECT_EQ(offsets[i], first + i);
}

// A single gap anywhere in the run makes the map non-affine
TEST(PackedPartOffsetsTest, ConsecutiveWithOneGap)
{
    PackedPartOffsets offsets;

    std::vector<UInt64> values = {100, 101, 102, 104, 105, 106};

    for (const auto & val : values)
        offsets.insert(val);
    offsets.flush();

    for (size_t i = 0; i < values.size(); ++i)
        EXPECT_EQ(offsets[i], values[i]);
}

// The gap is the step across a page boundary, which a per-page notion of consecutiveness misses
TEST(PackedPartOffsetsTest, GapAtPageBoundary)
{
    PackedPartOffsets offsets;

    constexpr UInt64 first = 500;
    const size_t count = 2 * TEST_PAGE_SIZE;

    std::vector<UInt64> values;
    values.reserve(count);
    for (size_t i = 0; i < count; ++i)
        values.push_back(first + i + (i >= TEST_PAGE_SIZE ? 1 : 0));

    for (const auto & val : values)
        offsets.insert(val);
    offsets.flush();

    for (size_t i = 0; i < count; ++i)
        EXPECT_EQ(offsets[i], values[i]);
}

TEST(PackedPartOffsetsTest, SingleValueConsecutive)
{
    PackedPartOffsets offsets;

    offsets.insert(777);
    offsets.flush();

    EXPECT_EQ(offsets[0], 777);
}

// Parts that are disjoint apart from a few interleaved rows: pages 0 and 2 and the partial tail hold
// consecutive values while page 1 carries a gap, so the pages of one map disagree
TEST(PackedPartOffsetsTest, MixedConsecutiveAndPackedPages)
{
    PackedPartOffsets offsets;

    const size_t tail = 500;
    std::vector<UInt64> values;
    values.reserve((3 * TEST_PAGE_SIZE) + tail);

    UInt64 current = 9000;
    for (size_t page = 0; page < 3; ++page)
    {
        for (size_t i = 0; i < TEST_PAGE_SIZE; ++i)
        {
            if (page == 1 && i == TEST_PAGE_SIZE / 2)
                current += 7;
            values.push_back(current);
            ++current;
        }
    }
    for (size_t i = 0; i < tail; ++i)
    {
        values.push_back(current);
        ++current;
    }

    for (const auto & val : values)
        offsets.insert(val);
    offsets.flush();

    for (size_t i = 0; i < values.size(); ++i)
        EXPECT_EQ(offsets[i], values[i]) << "offset " << i;
}

// A full page whose only non-unit step is the last one: its values span size(), not size() - 1
TEST(PackedPartOffsetsTest, ConsecutiveExceptLastValue)
{
    PackedPartOffsets offsets;

    constexpr UInt64 first = 42;
    std::vector<UInt64> values;
    values.reserve(TEST_PAGE_SIZE);
    for (size_t i = 0; i + 1 < TEST_PAGE_SIZE; ++i)
        values.push_back(first + i);
    values.push_back(values.back() + 2);

    for (const auto & val : values)
        offsets.insert(val);
    offsets.flush();

    for (size_t i = 0; i < values.size(); ++i)
        EXPECT_EQ(offsets[i], values[i]) << "offset " << i;
}

// Inserts the values of one part's offset map and finalizes it
static void insertAll(PackedPartOffsets & offsets, const std::vector<UInt64> & values)
{
    for (const auto & val : values)
        offsets.insert(val);
    offsets.flush();
}

// Maps the given offsets of a part in one call, and requires every result to be the one a single-offset
// lookup returns. Mapping a whole posting list is an optimization and must not change any value.
static void expectMapOffsetsMatchesLookup(
    const PackedPartOffsets & offsets, const std::vector<UInt32> & to_map, const std::string & layout)
{
    std::vector<UInt32> expected;
    expected.reserve(to_map.size());
    for (UInt32 offset : to_map)
        expected.push_back(static_cast<UInt32>(offsets[offset]));

    std::vector<UInt32> mapped = to_map;
    offsets.mapOffsets(mapped);

    for (size_t i = 0; i < mapped.size(); ++i)
    {
        if (mapped[i] != expected[i])
        {
            ADD_FAILURE() << layout << ": offset " << to_map[i] << " mapped to " << mapped[i] << ", expected "
                          << expected[i];
            return;
        }
    }
}

// Every offset from begin, count of them, stride apart, and none beyond size
static std::vector<UInt32> stridedOffsets(size_t begin, size_t stride, size_t count, size_t size)
{
    std::vector<UInt32> result;
    for (size_t i = 0, offset = begin; i < count && offset < size; ++i, offset += stride)
        result.push_back(static_cast<UInt32>(offset));
    return result;
}

// The offset maps a merge builds for one part: consecutive runs, runs broken at a page boundary,
// packed pages, and mixtures of them
static std::vector<std::pair<std::string, std::vector<UInt64>>> offsetMapLayouts()
{
    auto consecutive = [](UInt64 first, size_t count)
    {
        std::vector<UInt64> values(count);
        for (size_t i = 0; i < count; ++i)
            values[i] = first + i;
        return values;
    };

    std::vector<std::pair<std::string, std::vector<UInt64>>> layouts;

    layouts.emplace_back("one value", consecutive(5, 1));
    layouts.emplace_back("one partial page", consecutive(100, 700));
    layouts.emplace_back("exactly one page", consecutive(0, TEST_PAGE_SIZE));
    layouts.emplace_back("one page and one value", consecutive(7, TEST_PAGE_SIZE + 1));
    layouts.emplace_back("three pages and a tail", consecutive(12345, (3 * TEST_PAGE_SIZE) + 7));

    // Every page is consecutive, but the step across the first page boundary is not one
    {
        auto values = consecutive(500, 3 * TEST_PAGE_SIZE);
        for (size_t i = TEST_PAGE_SIZE; i < values.size(); ++i)
            values[i] += 1;
        layouts.emplace_back("gap at a page boundary", values);
    }

    // A gap inside the second page leaves the other pages consecutive
    {
        auto values = consecutive(9000, (3 * TEST_PAGE_SIZE) + 500);
        for (size_t i = TEST_PAGE_SIZE + (TEST_PAGE_SIZE / 2); i < values.size(); ++i)
            values[i] += 7;
        layouts.emplace_back("one packed page among consecutive ones", values);
    }

    // A gap inside the first page, with every later page consecutive
    {
        auto values = consecutive(0, 2 * TEST_PAGE_SIZE);
        for (size_t i = 3; i < values.size(); ++i)
            values[i] += 100;
        layouts.emplace_back("packed first page", values);
    }

    // The part takes every sixth row of the merged data, the shape of a fully interleaved merge
    {
        std::vector<UInt64> values((3 * TEST_PAGE_SIZE) + 13);
        for (size_t i = 0; i < values.size(); ++i)
            values[i] = 2 + (i * 6);
        layouts.emplace_back("every page packed", values);
    }

    return layouts;
}

// Mapping a posting list of a part in one call gives the same new row ids as mapping them one by one,
// for every shape of offset map a merge builds and for dense as well as sparse posting lists
TEST(PackedPartOffsetsTest, MapOffsetsMatchesLookup)
{
    for (const auto & [layout, values] : offsetMapLayouts())
    {
        PackedPartOffsets offsets;
        insertAll(offsets, values);
        const size_t size = values.size();

        for (size_t stride : {static_cast<size_t>(1), static_cast<size_t>(33), static_cast<size_t>(64), TEST_PAGE_SIZE})
        {
            expectMapOffsetsMatchesLookup(
                offsets, stridedOffsets(0, stride, size, size), layout + ", stride " + std::to_string(stride));
            expectMapOffsetsMatchesLookup(
                offsets, stridedOffsets(1, stride, size, size), layout + ", stride " + std::to_string(stride) + " from 1");
        }

        expectMapOffsetsMatchesLookup(offsets, stridedOffsets(size - 1, 1, 1, size), layout + ", last offset only");
        expectMapOffsetsMatchesLookup(offsets, {}, layout + ", no offsets");

        if (size > TEST_PAGE_SIZE)
            expectMapOffsetsMatchesLookup(
                offsets, stridedOffsets(TEST_PAGE_SIZE - 5, 1, 10, size), layout + ", across a page boundary");
    }
}

// Whether a posting list is dense enough to be mapped page by page must not change any new row id
TEST(PackedPartOffsetsTest, MapOffsetsDensityBoundary)
{
    // No page of a fully interleaved part is consecutive, so the density of the posting list picks the path
    std::vector<UInt64> values(4 * TEST_PAGE_SIZE);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = 3 + (i * 6);

    PackedPartOffsets offsets;
    insertAll(offsets, values);

    constexpr size_t pages_spanned = 3;
    constexpr size_t last = (pages_spanned * TEST_PAGE_SIZE) - 1;

    for (size_t count : {(pages_spanned * 32) - 1, pages_spanned * 32, (pages_spanned * 32) + 1})
    {
        // Spread the offsets over the same pages whatever their number is
        std::vector<UInt32> to_map;
        for (size_t i = 0; i < count; ++i)
            to_map.push_back(static_cast<UInt32>((i * last) / (count - 1)));

        expectMapOffsetsMatchesLookup(offsets, to_map, std::to_string(count) + " offsets over 3 pages");
    }
}

// Posting lists are sorted, but nothing in the mapping relies on it
TEST(PackedPartOffsetsTest, MapOffsetsUnsortedOffsets)
{
    std::vector<UInt64> values(3 * TEST_PAGE_SIZE);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = 1 + (i * 3);

    PackedPartOffsets offsets;
    insertAll(offsets, values);

    std::vector<UInt32> to_map;
    to_map.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i)
        to_map.push_back(static_cast<UInt32>(i));

    std::mt19937 gen(42); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
    std::shuffle(to_map.begin(), to_map.end(), gen);

    expectMapOffsetsMatchesLookup(offsets, to_map, "shuffled offsets");
}

// A part with no rows has no pages, and a token of it has no posting list to map
TEST(PackedPartOffsetsTest, MapOffsetsNoOffsetsNoPages)
{
    PackedPartOffsets offsets;
    std::vector<UInt32> to_map;

    offsets.mapOffsets(to_map);

    EXPECT_TRUE(to_map.empty());
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

// One merge where part 0 is not interleaved while parts 1 and 2 are interleaved with each other,
// so the parts of a single merge do not agree on whether their maps are affine
TEST(MergedPartOffsetsTest, MixedContiguousAndInterleavedParts)
{
    std::vector<UInt64> part_indices;

    for (size_t i = 0; i < 2000; ++i)
        part_indices.push_back(0);
    for (size_t i = 0; i < 2000; ++i)
    {
        part_indices.push_back(1);
        part_indices.push_back(2);
    }
    for (size_t i = 0; i < 100; ++i)
        part_indices.push_back(1);

    MergedPartOffsets merged_offsets(3);
    merged_offsets.insert(part_indices.data(), part_indices.data() + part_indices.size());
    merged_offsets.flush();

    std::vector<std::vector<UInt64>> expected(3);
    for (size_t i = 0; i < part_indices.size(); ++i)
        expected[part_indices[i]].push_back(i);

    for (size_t part = 0; part < expected.size(); ++part)
    {
        for (size_t i = 0; i < expected[part].size(); ++i)
            EXPECT_EQ((merged_offsets[part, i]), expected[part][i]) << "part " << part << ", offset " << i;
    }
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

// All rows of one part of a merge are mapped in one call, for a part that is not interleaved and for
// parts that are interleaved with each other
TEST(MergedPartOffsetsTest, MapOffsetsOfOnePart)
{
    std::vector<UInt64> part_indices;

    for (size_t i = 0; i < 3000; ++i)
        part_indices.push_back(0);
    for (size_t i = 0; i < 3000; ++i)
    {
        part_indices.push_back(1);
        part_indices.push_back(2);
    }

    MergedPartOffsets merged_offsets(3);
    merged_offsets.insert(part_indices.data(), part_indices.data() + part_indices.size());
    merged_offsets.flush();

    for (size_t part = 0; part < 3; ++part)
    {
        const size_t rows = merged_offsets.getPartRowsCount(part);

        std::vector<UInt32> expected;
        std::vector<UInt32> mapped;
        for (size_t i = 0; i < rows; ++i)
        {
            expected.push_back(static_cast<UInt32>((merged_offsets[part, i])));
            mapped.push_back(static_cast<UInt32>(i));
        }

        merged_offsets.mapOffsets(part, mapped);

        for (size_t i = 0; i < rows; ++i)
        {
            if (mapped[i] != expected[i])
            {
                ADD_FAILURE() << "part " << part << ", offset " << i << " mapped to " << mapped[i] << ", expected "
                              << expected[i];
                break;
            }
        }
    }
}
