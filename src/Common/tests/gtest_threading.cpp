#include <gtest/gtest.h>

#include <thread>
#include <condition_variable>
#include <shared_mutex>
#include <barrier>
#include <atomic>

#include "Common/Exception.h"
#include <Common/Threading.h>
#include <Common/Stopwatch.h>

#include <base/demangle.h>
#include <base/getThreadId.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int THREAD_WAS_CANCELLED;
    }
}

struct NoCancel {};

// for all PerfTests
static constexpr int requests = 512 * 1024;
static constexpr int max_threads = 16;

template <class T, class Status = NoCancel>
void TestSharedMutex()
{
    // Test multiple readers can acquire lock
    for (int readers = 1; readers <= 128; readers *= 2)
    {
        T sm;
        std::atomic<int> test(0);
        std::barrier sync(readers + 1);

        std::vector<std::thread> threads;
        threads.reserve(readers);
        auto reader = [&]
        {
            [[maybe_unused]] Status status;
            std::shared_lock lock(sm);
            sync.arrive_and_wait();
            test++;
        };

        for (int i = 0; i < readers; i++)
            threads.emplace_back(reader);

        { // writer
            [[maybe_unused]] Status status;
            sync.arrive_and_wait(); // wait for all reader to acquire lock to avoid blocking them
            std::unique_lock lock(sm);
            test++;
        }

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(test, readers + 1);
    }

    // Test multiple writers cannot acquire lock simultaneously
    for (int writers = 1; writers <= 128; writers *= 2)
    {
        T sm;
        int test = 0;
        std::barrier sync(writers);
        std::vector<std::thread> threads;

        threads.reserve(writers);
        auto writer = [&]
        {
            [[maybe_unused]] Status status;
            sync.arrive_and_wait();
            std::unique_lock lock(sm);
            test++;
        };

        for (int i = 0; i < writers; i++)
            threads.emplace_back(writer);

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(test, writers);
    }
}

template <class T, class Status = NoCancel>
void TestSharedMutexCancelReader()
{
    static constexpr int readers = 8;
    static constexpr int tasks_per_reader = 32;

    T sm;
    std::atomic<int> successes(0);
    std::atomic<int> cancels(0);
    std::barrier sync(readers + 1);
    std::barrier cancel_sync(readers / 2 + 1);
    std::vector<std::thread> threads;

    std::mutex m;
    std::vector<UInt64> tids_to_cancel;

    threads.reserve(readers);
    auto reader = [&] (int reader_id)
    {
        if (reader_id % 2 == 0)
        {
            std::unique_lock lock(m);
            tids_to_cancel.emplace_back(getThreadId());
        }
        for (int task = 0; task < tasks_per_reader; task++) {
            try
            {
                [[maybe_unused]] Status status;
                sync.arrive_and_wait(); // (A) sync with writer
                sync.arrive_and_wait(); // (B) wait for writer to acquire unique_lock
                std::shared_lock lock(sm);
                successes++;
            }
            catch (DB::Exception & e)
            {
                ASSERT_EQ(e.code(), DB::ErrorCodes::THREAD_WAS_CANCELLED);
                ASSERT_EQ(e.message(), "test");
                cancels++;
                cancel_sync.arrive_and_wait(); // (C) sync with writer
            }
        }
    };

    for (int reader_id = 0; reader_id < readers; reader_id++)
        threads.emplace_back(reader, reader_id);

    { // writer
        [[maybe_unused]] Status status;
        for (int task = 0; task < tasks_per_reader; task++) {
            sync.arrive_and_wait(); // (A) wait for readers to finish previous task
            ASSERT_EQ(cancels + successes, task * readers);
            ASSERT_EQ(cancels, task * readers / 2);
            ASSERT_EQ(successes, task * readers / 2);
            std::unique_lock lock(sm);
            sync.arrive_and_wait(); // (B) sync with readers
            //std::unique_lock lock(m); // not needed, already synced using barrier
            for (UInt64 tid : tids_to_cancel)
                DB::CancelToken::signal(tid, DB::ErrorCodes::THREAD_WAS_CANCELLED, "test");

            // This sync is crucial. It is needed to hold `lock` long enough.
            // It guarantees that every cancelled thread will find `sm` blocked by writer, and thus will begin to wait.
            // Wait() call is required for cancellation. Otherwise, fastpath acquire w/o wait will not generate exception.
            // And this is the desired behaviour.
            cancel_sync.arrive_and_wait(); // (C) wait for cancellation to finish, before unlock.
        }
    }

    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(successes, tasks_per_reader * readers / 2);
    ASSERT_EQ(cancels, tasks_per_reader * readers / 2);
}

template <class T, class Status = NoCancel>
void TestSharedMutexCancelWriter()
{
    static constexpr int writers = 8;
    static constexpr int tasks_per_writer = 32;

    T sm;
    std::atomic<int> successes(0);
    std::atomic<int> cancels(0);
    std::barrier sync(writers);
    std::vector<std::thread> threads;

    std::mutex m;
    std::vector<UInt64> all_tids;

    threads.reserve(writers);
    auto writer = [&]
    {
        {
            std::unique_lock lock(m);
            all_tids.emplace_back(getThreadId());
        }
        for (int task = 0; task < tasks_per_writer; task++) {
            try
            {
                [[maybe_unused]] Status status;
                sync.arrive_and_wait(); // (A) sync all threads before race to acquire the lock
                std::unique_lock lock(sm);
                successes++;
                // Thread that managed to acquire the lock cancels all other waiting writers
                //std::unique_lock lock(m); // not needed, already synced using barrier
                for (UInt64 tid : all_tids)
                {
                    if (tid != getThreadId())
                        DB::CancelToken::signal(tid, DB::ErrorCodes::THREAD_WAS_CANCELLED, "test");
                }

                // This sync is crucial. It is needed to hold `lock` long enough.
                // It guarantees that every cancelled thread will find `sm` blocked, and thus will begin to wait.
                // Wait() call is required for cancellation. Otherwise, fastpath acquire w/o wait will not generate exception.
                // And this is the desired behaviour.
                sync.arrive_and_wait(); // (B) wait for cancellation to finish, before unlock.
            }
            catch (DB::Exception & e)
            {
                ASSERT_EQ(e.code(), DB::ErrorCodes::THREAD_WAS_CANCELLED);
                ASSERT_EQ(e.message(), "test");
                cancels++;
                sync.arrive_and_wait(); // (B) sync with race winner
            }
        }
    };

    for (int writer_id = 0; writer_id < writers; writer_id++)
        threads.emplace_back(writer);

    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(successes, tasks_per_writer);
    ASSERT_EQ(cancels, tasks_per_writer * (writers - 1));
}

template <class T, class Status = NoCancel>
void PerfTestSharedMutexReadersOnly()
{
    std::cout << "*** " << demangle(typeid(T).name()) << "/" << demangle(typeid(Status).name()) << " ***" << std::endl;

    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        T sm;
        std::vector<std::thread> threads;
        threads.reserve(thrs);
        auto reader = [&]
        {
            [[maybe_unused]] Status status;
            for (int request = requests / thrs; request; request--)
            {
                std::shared_lock lock(sm);
            }
        };

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(reader);

        for (auto & thread : threads)
            thread.join();

        double ns = watch.elapsedNanoseconds();
        std::cout << "thrs = " << thrs << ":\t" << ns / requests << " ns\t" << requests * 1e9 / ns << " rps" << std::endl;
    }
}

template <class T, class Status = NoCancel>
void PerfTestSharedMutexWritersOnly()
{
    std::cout << "*** " << demangle(typeid(T).name()) << "/" << demangle(typeid(Status).name()) << " ***" << std::endl;

    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        int counter = 0;
        T sm;
        std::vector<std::thread> threads;
        threads.reserve(thrs);
        auto writer = [&]
        {
            [[maybe_unused]] Status status;
            for (int request = requests / thrs; request; request--)
            {
                std::unique_lock lock(sm);
                ASSERT_TRUE(counter % 2 == 0);
                counter++;
                std::atomic_signal_fence(std::memory_order::seq_cst); // force compiler to generate two separate increment instructions
                counter++;
            }
        };

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(writer);

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(counter, requests * 2);

        double ns = watch.elapsedNanoseconds();
        std::cout << "thrs = " << thrs << ":\t" << ns / requests << " ns\t" << requests * 1e9 / ns << " rps" << std::endl;
    }
}

template <class T, class Status = NoCancel>
void PerfTestSharedMutexRW()
{
    std::cout << "*** " << demangle(typeid(T).name()) << "/" << demangle(typeid(Status).name()) << " ***" << std::endl;

    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        int counter = 0;
        T sm;
        std::vector<std::thread> threads;
        threads.reserve(thrs);
        auto reader = [&]
        {
            [[maybe_unused]] Status status;
            for (int request = requests / thrs / 2; request; request--)
            {
                {
                    std::shared_lock lock(sm);
                    ASSERT_TRUE(counter % 2 == 0);
                }
                {
                    std::unique_lock lock(sm);
                    ASSERT_TRUE(counter % 2 == 0);
                    counter++;
                    std::atomic_signal_fence(std::memory_order::seq_cst); // force compiler to generate two separate increment instructions
                    counter++;
                }
            }
        };

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(reader);

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(counter, requests);

        double ns = watch.elapsedNanoseconds();
        std::cout << "thrs = " << thrs << ":\t" << ns / requests << " ns\t" << requests * 1e9 / ns << " rps" << std::endl;
    }
}

TEST(Threading, SharedMutexSmokeCancellableEnabled) { TestSharedMutex<DB::CancellableSharedMutex, DB::Cancellable>(); }
TEST(Threading, SharedMutexSmokeCancellableDisabled) { TestSharedMutex<DB::CancellableSharedMutex>(); }
TEST(Threading, SharedMutexSmokeFast) { TestSharedMutex<DB::FastSharedMutex>(); }
TEST(Threading, SharedMutexSmokeStd) { TestSharedMutex<std::shared_mutex>(); }

TEST(Threading, PerfTestSharedMutexReadersOnlyCancellableEnabled) { PerfTestSharedMutexReadersOnly<DB::CancellableSharedMutex, DB::Cancellable>(); }
TEST(Threading, PerfTestSharedMutexReadersOnlyCancellableDisabled) { PerfTestSharedMutexReadersOnly<DB::CancellableSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexReadersOnlyFast) { PerfTestSharedMutexReadersOnly<DB::FastSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexReadersOnlyStd) { PerfTestSharedMutexReadersOnly<std::shared_mutex>(); }

TEST(Threading, PerfTestSharedMutexWritersOnlyCancellableEnabled) { PerfTestSharedMutexWritersOnly<DB::CancellableSharedMutex, DB::Cancellable>(); }
TEST(Threading, PerfTestSharedMutexWritersOnlyCancellableDisabled) { PerfTestSharedMutexWritersOnly<DB::CancellableSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexWritersOnlyFast) { PerfTestSharedMutexWritersOnly<DB::FastSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexWritersOnlyStd) { PerfTestSharedMutexWritersOnly<std::shared_mutex>(); }

TEST(Threading, PerfTestSharedMutexRWCancellableEnabled) { PerfTestSharedMutexRW<DB::CancellableSharedMutex, DB::Cancellable>(); }
TEST(Threading, PerfTestSharedMutexRWCancellableDisabled) { PerfTestSharedMutexRW<DB::CancellableSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexRWFast) { PerfTestSharedMutexRW<DB::FastSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexRWStd) { PerfTestSharedMutexRW<std::shared_mutex>(); }

#ifdef OS_LINUX /// These tests require cancellability

TEST(Threading, SharedMutexCancelReaderCancellableEnabled) { TestSharedMutexCancelReader<DB::CancellableSharedMutex, DB::Cancellable>(); }
TEST(Threading, SharedMutexCancelWriterCancellableEnabled) { TestSharedMutexCancelWriter<DB::CancellableSharedMutex, DB::Cancellable>(); }

#endif
