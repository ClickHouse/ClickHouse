#include <gtest/gtest.h>

#include <Common/Arena.h>
#include <Common/Exception.h>

#include <limits>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

/// The size of an allocation can come from the data, and the arena adds the alignment, the padding
/// and the rounding on top of it before allocating a chunk. These additions would wrap around for a
/// size close to the maximum, and the chunk would be allocated smaller than the requested size.
TEST(Arena, TooLargeAllocationIsRejected)
{
    DB::Arena arena;

    auto expect_rejected = [](auto && allocate)
    {
        try
        {
            allocate();
            FAIL() << "The allocation was expected to be rejected";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
    };

    static constexpr size_t max_size = std::numeric_limits<size_t>::max();

    expect_rejected([&]{ arena.alloc(max_size); });
    expect_rejected([&]{ arena.alloc(max_size - 1024); });
    expect_rejected([&]{ arena.alignedAlloc(max_size - 1024, 64); });

    /// A size that the allocator refuses outright is reported as a data error rather than as the
    /// logical error the allocator would report.
    expect_rejected([&]{ arena.alignedAlloc(MAX_ALLOCATION_SIZE, 8); });

    /// Ordinary allocations are not affected.
    EXPECT_NE(arena.alloc(1024), nullptr);
    EXPECT_NE(arena.alignedAlloc(1024, 64), nullptr);
}

TEST(Arena, GrowthEstimationBoundsAllocations)
{
    std::vector<std::vector<size_t>> sequences{
        {}, {0}, {0, 0, 0}, {0, 1, 0, 64}, {3000, 3000, 3000},
        {4000, 4000}, {5000, 3000}, {1, 4033, 4097}, {4096, 8192, 16384},
        {50000, 40000, 65536, 1}, {200000, 1000, 200000}};
    /// A fixed seed keeps the allocation sequences reproducible.
    // NOLINTNEXTLINE(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
    std::mt19937 random(0);
    for (size_t i = 0; i < 500; ++i)
    {
        std::vector<size_t> sizes(random() % 32);
        for (auto & size : sizes)
            size = random() % 32768;
        sequences.push_back(std::move(sizes));
    }

    for (const size_t growth_factor : {1, 2, 3})
    {
        for (const size_t linear_growth_threshold : {5000, 32768, 134217728})
        {
            for (const size_t initial_allocation : {0, 3000, 40000})
            {
                for (const auto & sizes : sequences)
                {
                    SCOPED_TRACE(::testing::Message() << "growth_factor=" << growth_factor
                        << ", linear_growth_threshold=" << linear_growth_threshold
                        << ", initial_allocation=" << initial_allocation
                        << ", sizes=" << ::testing::PrintToString(sizes));
                    DB::Arena arena(4096, growth_factor, linear_growth_threshold);
                    if (initial_allocation)
                        arena.alloc(initial_allocation);
                    const size_t before_allocated = arena.allocatedBytes();
                    const size_t before_used = arena.usedBytes();
                    const size_t total_bytes = std::accumulate(sizes.begin(), sizes.end(), size_t(0));
                    const size_t estimate = arena.estimateGrowthMemory(sizes.size(), total_bytes);
                    EXPECT_EQ(arena.allocatedBytes(), before_allocated);
                    EXPECT_EQ(arena.usedBytes(), before_used);
                    for (const auto size : sizes)
                        arena.alloc(size);

                    const size_t buffer_growth = arena.allocatedBytes() - before_allocated;
                    EXPECT_GE(estimate, buffer_growth);
                    if (buffer_growth == 0)
                        EXPECT_EQ(estimate, 0);
                }
            }
        }
    }
}

TEST(Arena, GrowthEstimationSaturatesAtRepresentableSize)
{
    constexpr size_t max_size = std::numeric_limits<size_t>::max();
    for (const size_t linear_growth_threshold : {5000, 32768, 134217728})
    {
        for (const size_t initial_allocation : {0, 3000, 40000})
        {
            DB::Arena arena(4096, 2, linear_growth_threshold);
            if (initial_allocation)
                arena.alloc(initial_allocation);
            const size_t before_allocated = arena.allocatedBytes();
            const size_t before_used = arena.usedBytes();
            for (const auto & [allocations, bytes] : {
                     std::pair{size_t(1), max_size}, std::pair{size_t(2), max_size},
                     std::pair{size_t(2), max_size / 2}, std::pair{max_size, max_size}})
                EXPECT_EQ(arena.estimateGrowthMemory(allocations, bytes), max_size);

            /// Estimates can exceed the single-allocation limit while still fitting in `size_t`.
            const size_t large_estimate = arena.estimateGrowthMemory(1, MAX_ALLOCATION_SIZE);
            EXPECT_GT(large_estimate, MAX_ALLOCATION_SIZE);
            EXPECT_LT(large_estimate, max_size);
            EXPECT_EQ(arena.allocatedBytes(), before_allocated);
            EXPECT_EQ(arena.usedBytes(), before_used);
        }
    }
}

TEST(Arena, GrowthEstimationSaturatesBufferSizing)
{
    constexpr size_t max_size = std::numeric_limits<size_t>::max();
    DB::Arena large_initial_buffer(max_size);
    EXPECT_EQ(large_initial_buffer.estimateGrowthMemory(1, 1), max_size);
    EXPECT_EQ(large_initial_buffer.allocatedBytes(), 0);

    DB::Arena large_growth_factor(4096, max_size);
    large_growth_factor.alloc(1);
    const size_t before_allocated = large_growth_factor.allocatedBytes();
    const size_t before_used = large_growth_factor.usedBytes();
    EXPECT_EQ(large_growth_factor.estimateGrowthMemory(1, before_allocated), max_size);
    EXPECT_EQ(large_growth_factor.allocatedBytes(), before_allocated);
    EXPECT_EQ(large_growth_factor.usedBytes(), before_used);
}
