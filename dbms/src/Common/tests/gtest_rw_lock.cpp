#include <gtest/gtest.h>

#include <Common/RWLock.h>
#include <Common/Stopwatch.h>
#include <common/Types.h>
#include <Common/ThreadPool.h>
#include <random>
#include <pcg_random.hpp>
#include <thread>
#include <atomic>
#include <iomanip>
#include <mutex>


namespace
{

class Barrier
{
public:
    explicit Barrier(const size_t num_threads) : num_threads{num_threads} {}
    Barrier(const Barrier&) = delete;
    Barrier& operator=(const Barrier&) = delete;

    void arrive_and_wait();

private:
    const size_t num_threads;
    std::mutex mutex;
    std::condition_variable next_epoch_cv;
};

}


using namespace DB;


TEST(Common, RWLock_1)
{
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

TEST(Common, RWLock_Recursive)
{
    constexpr auto cycles = 10000;

    static auto fifo_lock = RWLockImpl::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    std::thread t1([&] ()
    {
        for (int i = 0; i < 2 * cycles; ++i)
        {
            auto lock = fifo_lock->getLock(RWLockImpl::Write, RWLockImpl::NO_QUERY);

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);
        }
    });

    std::thread t2([&] ()
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto lock1 = fifo_lock->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY);

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);

            auto lock2 = fifo_lock->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY);

            EXPECT_ANY_THROW({fifo_lock->getLock(RWLockImpl::Write, RWLockImpl::NO_QUERY);});
        }

        fifo_lock->getLock(RWLockImpl::Write, RWLockImpl::NO_QUERY);
    });

    t1.join();
    t2.join();
}


void Barrier::arrive_and_wait()
{
    static bool epoch = false;
    static size_t threads_countdown = num_threads;

    std::unique_lock<std::mutex> lock(mutex);

    if (--threads_countdown > 0)
    {
        const auto my_epoch = epoch;
        while (my_epoch == epoch)
        {
            next_epoch_cv.wait(lock);
        }
    }
    else
    {
        threads_countdown = num_threads;
        epoch = !epoch;
        next_epoch_cv.notify_all();
    }
}


TEST(Common, RWLock_Recursive_WithQueryContext)
{
    constexpr auto burst_size = 400;
    constexpr auto bursts_count = 1000;

    static const String query_id_1 = "query-id-1";
    static const String query_id_2 = "query-id-2";

    auto fifo_lock = RWLockImpl::create();

    std::atomic_int readers = 0;
    std::atomic_int writers = 0;

    Barrier barrier(3);  /// For syncronized start of probing sequences

    std::thread t1([&] ()
    {
        for (int i = 0; i < bursts_count; ++i)
        {
            barrier.arrive_and_wait();

            for (int j = 0; j < burst_size; ++j)
            {
                auto lock = fifo_lock->getLock(RWLockImpl::Read, query_id_1);

                EXPECT_EQ(writers.load(), 0);
                EXPECT_LE(readers.fetch_add(1), 1);

                std::this_thread::yield();

                EXPECT_LE(readers.fetch_sub(1), 2);
            }
        }
    });

    std::thread t2([&] ()
    {
        for (int i = 0; i < bursts_count; ++i)
        {
            barrier.arrive_and_wait();

            for (int j = 0; j < burst_size; ++j)
            {
                auto lock1 = fifo_lock->getLock(RWLockImpl::Read, query_id_1);

                EXPECT_EQ(writers.load(), 0);
                EXPECT_LE(readers.fetch_add(1), 1);

                std::this_thread::yield();

                auto lock2 = fifo_lock->getLock(RWLockImpl::Read, query_id_1);

                EXPECT_LE(readers.fetch_sub(1), 2);
            }
        }
    });

    std::thread t3([&] ()
    {
        for (int i = 0; i < bursts_count; ++i)
        {
            barrier.arrive_and_wait();

            for (int j = 0; j < burst_size; ++j)
            {
                auto lock1 = fifo_lock->getLock(RWLockImpl::Write, query_id_2);

                EXPECT_EQ(readers.load(), 0);
                EXPECT_EQ(writers.fetch_add(1), 0);

                std::this_thread::yield();

                EXPECT_EQ(writers.fetch_sub(1), 1);
                EXPECT_EQ(readers.load(), 0);
            }
        }
    });

    t1.join();
    t2.join();
    t3.join();
}


TEST(Common, RWLock_PerfTest_Readers)
{
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
