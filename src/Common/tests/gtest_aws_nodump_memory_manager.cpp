#ifdef USE_AWS_S3
#ifdef USE_JEMALLOC
#include <gtest/gtest.h>
#include <cstdint>

#include <Common/AwsNodumpMemoryManager.h>

using namespace DB;

TEST(AwsNodumpMemoryManagerTest, BasicAllocation)
{
    AwsNodumpMemoryManager allocator;
    void * ptr = allocator.AllocateMemory(128, 0, nullptr);
    ASSERT_NE(ptr, nullptr);
    memset(ptr, 0, 128);
    ASSERT_NO_THROW(allocator.FreeMemory(ptr));
}

TEST(AwsNodumpMemoryManagerTest, AlignedAllocation)
{
    AwsNodumpMemoryManager allocator;
    const size_t alignment = 64;
    void * ptr = allocator.AllocateMemory(256, alignment, nullptr);

    ASSERT_NE(ptr, nullptr);
    uintptr_t ptr_address = reinterpret_cast<uintptr_t>(ptr);
    EXPECT_EQ(ptr_address % alignment, 0);

    ASSERT_NO_THROW(allocator.FreeMemory(ptr));
}

#endif
#endif
