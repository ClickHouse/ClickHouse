#include <iostream>
#include <stdexcept>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>


static bool check()
{
    ThreadPool pool(10);

    pool.scheduleOrThrowOnError([] { throw std::runtime_error("Hello, world!"); });

    try
    {
        for (size_t i = 0; i < 500; ++i)
            pool.scheduleOrThrowOnError([] {});    /// An exception will be rethrown from this method.
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
