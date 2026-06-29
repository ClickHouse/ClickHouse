#include <gtest/gtest.h>

#include <Common/AllocatorWithMemoryTracking.h>

#include <limits>
#include <list>
#include <new>
#include <utility>

/// The byte counter must not move when the underlying allocation throws, otherwise the
/// cache-bytes metrics would drift upward permanently after a failed allocation.
TEST(BytesAwareAllocatorWithMemoryTracking, AllocateThrowDoesNotChangeCounter)
{
    BytesAwareAllocatorWithMemoryTracking<int> alloc;
    EXPECT_EQ(alloc.getBytesAllocated(), 0u);

    /// `n * sizeof(int)` overflows, so the delegate allocator throws before allocating.
    EXPECT_THROW(std::ignore = alloc.allocate(std::numeric_limits<size_t>::max()), std::bad_alloc);

    EXPECT_EQ(alloc.getBytesAllocated(), 0u);
}

TEST(BytesAwareAllocatorWithMemoryTracking, TracksContainerAllocations)
{
    std::list<int, BytesAwareAllocatorWithMemoryTracking<int>> list;
    EXPECT_EQ(list.get_allocator().getBytesAllocated(), 0u);

    list.push_back(1);
    list.push_back(2);
    const size_t two_nodes = list.get_allocator().getBytesAllocated();
    EXPECT_GT(two_nodes, 0u);

    list.pop_back();
    EXPECT_LT(list.get_allocator().getBytesAllocated(), two_nodes);
    EXPECT_GT(list.get_allocator().getBytesAllocated(), 0u);
}

/// A copied container must own a fresh counter that reflects only its own bytes, and must
/// not inflate the source's counter (which the previous shared-counter implementation did).
TEST(BytesAwareAllocatorWithMemoryTracking, CopyDoesNotDoubleCount)
{
    std::list<int, BytesAwareAllocatorWithMemoryTracking<int>> source;
    source.push_back(1);
    source.push_back(2);
    source.push_back(3);

    const size_t source_bytes = source.get_allocator().getBytesAllocated();
    EXPECT_GT(source_bytes, 0u);

    auto copy = source;
    EXPECT_EQ(source.get_allocator().getBytesAllocated(), source_bytes);
    EXPECT_EQ(copy.get_allocator().getBytesAllocated(), source_bytes);

    /// Growing the copy must not touch the source's counter.
    copy.push_back(4);
    EXPECT_EQ(source.get_allocator().getBytesAllocated(), source_bytes);
    EXPECT_GT(copy.get_allocator().getBytesAllocated(), source_bytes);
}

TEST(BytesAwareAllocatorWithMemoryTracking, MoveTransfersCounter)
{
    std::list<int, BytesAwareAllocatorWithMemoryTracking<int>> source;
    source.push_back(1);
    source.push_back(2);
    const size_t source_bytes = source.get_allocator().getBytesAllocated();

    auto moved = std::move(source);
    EXPECT_EQ(moved.get_allocator().getBytesAllocated(), source_bytes);
}
