#include "config.h"

#if USE_JEMALLOC

#include <gtest/gtest.h>
#include <vector>
#include <string>

#include <Common/JemallocNodumpSTLAllocator.h>

using namespace DB;

TEST(JemallocNodumpSTLAllocatorTest, AllocateAndDeallocateSingleObject)
{
    JemallocNodumpSTLAllocator<int> alloc;
    int * p = nullptr;

    ASSERT_NO_THROW(p = alloc.allocate(1));
    ASSERT_NE(p, nullptr);

    *p = 42;
    EXPECT_EQ(*p, 42);

    ASSERT_NO_THROW(alloc.deallocate(p, 1));
}

TEST(JemallocNodumpSTLAllocatorTest, AllocateArray)
{
    JemallocNodumpSTLAllocator<char> allocator;
    const size_t array_size = 512;
    char * buffer = nullptr;

    ASSERT_NO_THROW(buffer = allocator.allocate(array_size));
    ASSERT_NE(buffer, nullptr);

    for (size_t i = 0; i < array_size; ++i)
    {
        buffer[i] = static_cast<char>(i % 128);
    }
    for (size_t i = 0; i < array_size; ++i)
    {
        EXPECT_EQ(buffer[i], static_cast<char>(i % 128));
    }

    ASSERT_NO_THROW(allocator.deallocate(buffer, array_size));
}

TEST(JemallocNodumpSTLAllocatorTest, VectorWithInts)
{
    std::vector<int, JemallocNodumpSTLAllocator<int>> vec;
    for (int i = 0; i < 100; ++i)
    {
        vec.push_back(i);
    }

    ASSERT_EQ(vec.size(), 100);
    for (int i = 0; i < 100; ++i)
    {
        EXPECT_EQ(vec[i], i);
    }
}

TEST(JemallocNodumpSTLAllocatorTest, VectorWithStrings)
{
    std::vector<std::string, JemallocNodumpSTLAllocator<std::string>> vec;
    vec.push_back("hello");
    vec.push_back("world");

    ASSERT_EQ(vec.size(), 2);
    EXPECT_EQ(vec[0], "hello");
    EXPECT_EQ(vec[1], "world");
}

TEST(JemallocNodumpSTLAllocatorTest, ThrowsOnTooLargeAllocation)
{
    JemallocNodumpSTLAllocator<int> allocator;
    const size_t too_large_size = std::numeric_limits<std::size_t>::max();
    EXPECT_THROW(allocator.allocate(too_large_size), std::bad_alloc);
}

TEST(JemallocNodumpSTLAllocatorTest, EqualityOperators)
{
    JemallocNodumpSTLAllocator<int> alloc1;
    JemallocNodumpSTLAllocator<int> alloc2;
    JemallocNodumpSTLAllocator<double> alloc3;

    EXPECT_TRUE(alloc1 == alloc2);
    EXPECT_FALSE(alloc1 != alloc2);

    EXPECT_TRUE(alloc1 == alloc3);
    EXPECT_FALSE(alloc1 != alloc3);
}
#endif
