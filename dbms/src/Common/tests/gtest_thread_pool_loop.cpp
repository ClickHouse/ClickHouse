#include <atomic>
#include <iostream>
#include <Common/ThreadPool.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
    #pragma clang diagnostic ignored "-Wundef"
#endif
#include <gtest/gtest.h>


TEST(ThreadPool, Loop)
{
    std::atomic<int> res{0};

    for (size_t i = 0; i < 1000; ++i)
    {
        size_t threads = 16;
        ThreadPool pool(threads);
        for (size_t j = 0; j < threads; ++j)
            pool.schedule([&]{ ++res; });
        pool.wait();
    }

    EXPECT_EQ(res, 16000);
}
