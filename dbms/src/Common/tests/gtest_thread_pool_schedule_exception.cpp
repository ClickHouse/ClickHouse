#include <iostream>
#include <stdexcept>
#include <Common/ThreadPool.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
    #pragma clang diagnostic ignored "-Wundef"
#endif
#include <gtest/gtest.h>


bool check()
{
    ThreadPool pool(10);

    pool.schedule([]{ throw std::runtime_error("Hello, world!"); });

    try
    {
        for (size_t i = 0; i < 100; ++i)
            pool.schedule([]{});    /// An exception will be rethrown from this method.
    }
    catch (const std::runtime_error &)
    {
        return true;
    }

    pool.wait();

    return false;
}


TEST(ThreadPool, ExceptionFromSchedule)
{
    EXPECT_TRUE(check());
}
