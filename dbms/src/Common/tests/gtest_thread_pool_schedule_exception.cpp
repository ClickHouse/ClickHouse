#include <iostream>
#include <stdexcept>
#include <Common/ThreadPool.h>

#if !__clang__
#pragma GCC diagnostic ignored "-Wsuggest-override"
#endif
#include <gtest/gtest.h>


static bool check()
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
