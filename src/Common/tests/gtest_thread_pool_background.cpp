#include <atomic>
#include <iostream>
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Core/UUID.h>

#include <gtest/gtest.h>


namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
}

TEST(BackgroundThreadPool, Exceptions)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");
    BackgroundThreadPool pool(16, CurrentMetrics::BackgroundPoolTask);
    pool.scheduleOrThrowOnShutdown([&] { throw DB::Exception(DB::ErrorCodes::UNSUPPORTED_METHOD, "Some Exception"); });

    std::atomic<int> res{0};

    for (size_t i = 0; i < 1000; ++i)
        pool.scheduleOrThrowOnShutdown([&] { ++res; });

    pool.wait();
    Poco::Logger::root().setLevel("none");

    EXPECT_EQ(res, 1000);
}

TEST(BackgroundThreadPool, JobGroup)
{
    std::atomic<int> res1{0};
    std::atomic<int> res2{0};
    DB::UUID first_group = DB::UUIDHelpers::generateV4();
    DB::UUID second_group = DB::UUIDHelpers::generateV4();

    BackgroundThreadPool pool(16, CurrentMetrics::BackgroundPoolTask);
    for (size_t i = 0; i < 16; ++i)
    {
        if (i % 2 == 0)
            pool.scheduleOrThrowOnShutdown([&] { sleep(1); ++res1; }, first_group);
        else
            pool.scheduleOrThrowOnShutdown([&] { sleep(3); ++res2; }, second_group);
    }
    pool.waitGroup(first_group);
    EXPECT_EQ(res1, 8);
    EXPECT_EQ(res2, 0);
    pool.waitGroup(second_group);
    EXPECT_EQ(res1, 8);
    EXPECT_EQ(res2, 8);
}

TEST(BackgroundThreadPool, Metric)
{

    BackgroundThreadPool pool(16, CurrentMetrics::BackgroundPoolTask);
    for (size_t i = 0; i < 16; ++i)
        pool.scheduleOrThrowOnShutdown([&] { sleep(5); });

    bool found = false;
    /// Pool start jobs execution asynchronously, so we just trying to catch the right moment.
    for (size_t i = 0; i < 10000; ++i)
    {
        if (CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask].load() == 16)
        {
            found = true;
            break;
        }
        sleep(0.1);
    }

    EXPECT_TRUE(found);

    pool.wait();
}
