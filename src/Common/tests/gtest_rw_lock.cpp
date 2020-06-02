#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Common/Stopwatch.h>
#include <common/types.h>
#include <Common/ThreadPool.h>
#include <common/phdr_cache.h>
#include <random>
#include <pcg_random.hpp>
#include <thread>
#include <atomic>
#include <iomanip>


using namespace DB;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int DEADLOCK_AVOIDED;
    }
}


TEST(Common, RWLock1)
{
    /// Tests with threads require this, because otherwise
    ///  when tested under Memory Sanitizer,
    ///  it tries to obtain stack trace on 'free' invocation at thread exit,
    ///  but cannot do that due to infinite recursion.
    /// Alternative solution: disable PHDR Cache under memory sanitizer.
    updatePHDRCache();

    constexpr int cycles = 1000;
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static std::atomic<int> readers{0};
    static std::atomic<int> writers{0};

    static auto fifo_lock = RWLockImpl::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    auto func = [&] (size_t threads, int round)
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto type = (std::uniform_int_distribution<>(0, 9)(gen) >= round) ? RWLockImpl::Read : RWLockImpl::Write;
            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));

            auto lock = fifo_lock->getLock(type, RWLockImpl::NO_QUERY);

            if (type == RWLockImpl::Write)
            {
                ++writers;

                ASSERT_EQ(writers, 1);
                ASSERT_EQ(readers, 0);

                std::this_thread::sleep_for(sleep_for);

                --writers;
            }
            else
            {
                ++readers;

                ASSERT_EQ(writers, 0);
                ASSERT_GE(readers, 1);
                ASSERT_LE(readers, threads);

                std::this_thread::sleep_for(sleep_for);

                --readers;
            }
        }
    };

    for (auto pool_size : pool_sizes)
    {
        for (int round = 0; round < 10; ++round)
        {
            Stopwatch watch(CLOCK_MONOTONIC_COARSE);

            std::list<std::thread> threads;
            for (size_t thread = 0; thread < pool_size; ++thread)
                threads.emplace_back([=] () { func(pool_size, round); });

            for (auto & thread : threads)
                thread.join();

            auto total_time = watch.elapsedSeconds();
            std::cout << "Threads " << pool_size << ", round " << round << ", total_time " << std::setprecision(2) << total_time << "\n";
        }
    }
}

TEST(Common, RWLockRecursive)
{
    updatePHDRCache();

    constexpr auto cycles = 10000;

    static auto fifo_lock = RWLockImpl::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    std::thread t1([&] ()
    {
        for (int i = 0; i < 2 * cycles; ++i)
        {
            auto lock = fifo_lock->getLock(RWLockImpl::Write, "q1");

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);
        }
    });

    std::thread t2([&] ()
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto lock1 = fifo_lock->getLock(RWLockImpl::Read, "q2");

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);

            auto lock2 = fifo_lock->getLock(RWLockImpl::Read, "q2");

            EXPECT_ANY_THROW({fifo_lock->getLock(RWLockImpl::Write, "q2");});
        }

        fifo_lock->getLock(RWLockImpl::Write, "q2");
    });

    t1.join();
    t2.join();
}


TEST(Common, RWLockDeadlock)
{
    updatePHDRCache();

    static auto lock1 = RWLockImpl::create();
    static auto lock2 = RWLockImpl::create();

    /**
      * q1: r1          r2
      * q2:    w1
      * q3:       r2       r1
      * q4:          w2
      */

    std::thread t1([&] ()
    {
        auto holder1 = lock1->getLock(RWLockImpl::Read, "q1");
        usleep(100000);
        usleep(100000);
        usleep(100000);
        usleep(100000);
        try
        {
            auto holder2 = lock2->getLock(RWLockImpl::Read, "q1", std::chrono::milliseconds(100));
            if (!holder2)
            {
                throw Exception(
                        "Locking attempt timed out! Possible deadlock avoided. Client should retry.",
                        ErrorCodes::DEADLOCK_AVOIDED);
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DEADLOCK_AVOIDED)
                throw;
        }
    });

    std::thread t2([&] ()
    {
        usleep(100000);
        auto holder1 = lock1->getLock(RWLockImpl::Write, "q2");
    });

    std::thread t3([&] ()
    {
        usleep(100000);
        usleep(100000);
        auto holder2 = lock2->getLock(RWLockImpl::Read, "q3");
        usleep(100000);
        usleep(100000);
        usleep(100000);
        try
        {
            auto holder1 = lock1->getLock(RWLockImpl::Read, "q3", std::chrono::milliseconds(100));
            if (!holder1)
            {
                throw Exception(
                        "Locking attempt timed out! Possible deadlock avoided. Client should retry.",
                        ErrorCodes::DEADLOCK_AVOIDED);
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DEADLOCK_AVOIDED)
                throw;
        }
    });

    std::thread t4([&] ()
    {
        usleep(100000);
        usleep(100000);
        usleep(100000);
        auto holder2 = lock2->getLock(RWLockImpl::Write, "q4");
    });

    t1.join();
    t2.join();
    t3.join();
    t4.join();
}


TEST(Common, RWLockPerfTestReaders)
{
    updatePHDRCache();

    constexpr int cycles = 100000; // 100k
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static auto fifo_lock = RWLockImpl::create();

    for (auto pool_size : pool_sizes)
    {
            Stopwatch watch(CLOCK_MONOTONIC_COARSE);

            auto func = [&] ()
            {
                for (auto i = 0; i < cycles; ++i)
                {
                    auto lock = fifo_lock->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY);
                }
            };

            std::list<std::thread> threads;
            for (size_t thread = 0; thread < pool_size; ++thread)
                threads.emplace_back(func);

            for (auto & thread : threads)
                thread.join();

            auto total_time = watch.elapsedSeconds();
            std::cout << "Threads " << pool_size << ", total_time " << std::setprecision(2) << total_time << "\n";
    }
}
