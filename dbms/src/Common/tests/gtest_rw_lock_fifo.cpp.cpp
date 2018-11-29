#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif
#include <gtest/gtest.h>

#include <Common/RWLockFIFO.h>
#include <Common/Stopwatch.h>
#include <common/Types.h>
#include <common/ThreadPool.h>
#include <random>
#include <pcg_random.hpp>
#include <thread>
#include <atomic>
#include <iomanip>


using namespace DB;


TEST(Common, RWLockFIFO_1)
{
    constexpr int cycles = 1000;
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static std::atomic<int> readers{0};
    static std::atomic<int> writers{0};

    static auto fifo_lock = RWLockFIFO::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    auto func = [&] (size_t threads, int round)
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto type = (std::uniform_int_distribution<>(0, 9)(gen) >= round) ? RWLockFIFO::Read : RWLockFIFO::Write;
            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));

            auto lock = fifo_lock->getLock(type, "RW");

            if (type == RWLockFIFO::Write)
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

TEST(Common, RWLockFIFO_Recursive)
{
    constexpr auto cycles = 10000;

    static auto fifo_lock = RWLockFIFO::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    std::thread t1([&] ()
    {
        for (int i = 0; i < 2 * cycles; ++i)
        {
            auto lock = fifo_lock->getLock(RWLockFIFO::Write);

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);
        }
    });

    std::thread t2([&] ()
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto lock1 = fifo_lock->getLock(RWLockFIFO::Read);

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);

            auto lock2 = fifo_lock->getLock(RWLockFIFO::Read);

            EXPECT_ANY_THROW({fifo_lock->getLock(RWLockFIFO::Write);});
        }

        fifo_lock->getLock(RWLockFIFO::Write);
    });

    t1.join();
    t2.join();
}


TEST(Common, RWLockFIFO_PerfTest_Readers)
{
    constexpr int cycles = 100000; // 100k
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static auto fifo_lock = RWLockFIFO::create();

    for (auto pool_size : pool_sizes)
    {
            Stopwatch watch(CLOCK_MONOTONIC_COARSE);

            auto func = [&] ()
            {
                for (auto i = 0; i < cycles; ++i)
                {
                    auto lock = fifo_lock->getLock(RWLockFIFO::Read);
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
