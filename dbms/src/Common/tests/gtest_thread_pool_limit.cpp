#include <atomic>
#include <iostream>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

/// Test for thread self-removal when number of free threads in pool is too large.
/// Just checks that nothing weird happens.

template <typename Pool>
int test()
{
    Pool pool(10, 2, 10);

    std::atomic<int> counter{0};
    for (size_t i = 0; i < 10; ++i)
        pool.scheduleOrThrowOnError([&]{ ++counter; });
    pool.wait();

    return counter;
}

TEST(ThreadPool, ThreadRemoval)
{
    EXPECT_EQ(test<FreeThreadPool>(), 10);
    EXPECT_EQ(test<ThreadPool>(), 10);
}
