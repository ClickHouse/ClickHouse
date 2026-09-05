#include <gtest/gtest.h>

#include <Common/Arena.h>
#include <Common/Exception.h>

#include <limits>

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
