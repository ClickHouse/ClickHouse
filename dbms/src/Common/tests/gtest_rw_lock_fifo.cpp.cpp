#include <gtest/gtest.h>
#include <Common/RWLockFIFO.h>
#include <Common/Stopwatch.h>
#include <common/Types.h>
#include <common/ThreadPool.h>
#include <random>
#include <thread>
#include <atomic>
#include <iomanip>


using namespace DB;

static void execute_1(size_t threads, int round, int cycles)
{
    static std::atomic<int> readers{0};
    static std::atomic<int> writers{0};

    static auto fifo_lock = RWLockFIFO::create();

    static __thread std::random_device rd;
    static __thread std::mt19937 gen(rd());

    for (int  i = 0; i < cycles; ++i)
    {
        auto type = (std::uniform_int_distribution<>(0, 9)(gen) >= round) ? RWLockFIFO::Read : RWLockFIFO::Write;
        auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 5)(gen));

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
}

TEST(Common, RWLockFIFO_1)
{
    constexpr int cycles = 10000;

    for (size_t pool_size = 1; pool_size < 8; ++pool_size)
    {
        for (int round = 0; round < 10; ++round)
        {
            Stopwatch watch(CLOCK_MONOTONIC_COARSE);

            std::vector<std::thread> threads;
            threads.reserve(pool_size);
            for (int thread = 0; thread < pool_size; ++thread)
                threads.emplace_back([=] () { execute_1(pool_size, round, cycles); });

            for (auto & thread : threads)
                thread.join();

            auto total_time = watch.elapsedSeconds();
            std::cout << "Threads " << pool_size << ", round " << round << ", total_time " << std::setprecision(2) << total_time << "\n";
        }
    }
}
