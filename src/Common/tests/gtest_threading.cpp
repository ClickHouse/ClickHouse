#include <gtest/gtest.h>

#include <thread>
#include <condition_variable>
#include <shared_mutex>
#include <barrier>
#include <atomic>

#include <Common/Exception.h>
#include <Common/SharedMutex.h>
#include <Common/Stopwatch.h>

#include <base/demangle.h>

#include <absl/synchronization/mutex.h>

namespace DB
{

class TSA_CAPABILITY("AbslSharedMutex") AbslSharedMutex final : private absl::Mutex
{
    using absl::Mutex::Mutex;

public:
    AbslSharedMutex(const AbslSharedMutex &) = delete;
    AbslSharedMutex & operator=(const AbslSharedMutex &) = delete;
    AbslSharedMutex(AbslSharedMutex &&) = delete;
    AbslSharedMutex & operator=(AbslSharedMutex &&) = delete;

    // Exclusive ownership
    void lock() TSA_ACQUIRE() { WriterLock(); }

    bool try_lock() TSA_TRY_ACQUIRE(true) { return WriterTryLock(); }

    void unlock() TSA_RELEASE() { WriterUnlock(); }

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED() { ReaderLock(); }

    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true) { return ReaderTryLock(); }

    void unlock_shared() TSA_RELEASE_SHARED() { ReaderUnlock(); }
};
}

#ifdef OS_LINUX

namespace DB
{
class TSA_CAPABILITY("SelfSharedMutex") SelfSharedMutex final : public SharedMutex
{
};
}
#endif


struct NoCancel {};

// for all PerfTests
static constexpr int requests = 256 * 1024;
static constexpr int max_threads = 128;

template <class T, class Status = NoCancel>
void TestSharedMutex()
{
    // Test multiple readers can acquire lock
    for (int readers = 1; readers <= 128; readers *= 2)
    {
        T sm;
        std::atomic<int> test(0);
        std::barrier<std::__empty_completion> sync(readers + 1);

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
        std::barrier<std::__empty_completion> sync(writers);
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

    // Test multiple readers can acquire lock simultaneously using try_shared_lock
    for (int readers = 1; readers <= 128; readers *= 2)
    {
        T sm;
        std::atomic<int> test(0);
        std::barrier<std::__empty_completion> sync(readers + 1);

        std::vector<std::thread> threads;
        threads.reserve(readers);
        auto reader = [&]
        {
            [[maybe_unused]] Status status;
            bool acquired = sm.try_lock_shared();
            ASSERT_TRUE(acquired);
            if (!acquired) return; // Just to make TSA happy
            sync.arrive_and_wait(); // (A) sync with writer
            test++;
            sync.arrive_and_wait(); // (B) wait for writer to call try_lock() while shared_lock is held
            sm.unlock_shared();
            sync.arrive_and_wait(); // (C) wait for writer to release lock, to ensure try_lock_shared() will see no writer
        };

        for (int i = 0; i < readers; i++)
            threads.emplace_back(reader);

        { // writer
            [[maybe_unused]] Status status;
            sync.arrive_and_wait(); // (A) wait for all reader to acquire lock to avoid blocking them
            ASSERT_FALSE(sm.try_lock());
            sync.arrive_and_wait(); // (B) sync with readers
            {
                std::unique_lock lock(sm);
                test++;
            }
            sync.arrive_and_wait(); // (C) sync with readers
        }

        for (auto & thread : threads)
            thread.join();

        ASSERT_EQ(test, readers + 1);
    }
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

// static_assert(sizeof(DB::SelfSharedMutex) == 16);

#ifdef OS_LINUX
TEST(Threading, SharedMutexSmokeSelf) { TestSharedMutex<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, SharedMutexSmokeAbsl) { TestSharedMutex<DB::AbslSharedMutex>(); }
TEST(Threading, SharedMutexSmokeStd) { TestSharedMutex<std::shared_mutex>(); }

#ifdef OS_LINUX
TEST(Threading, PerfTestSharedMutexReadersOnlySelf) { PerfTestSharedMutexReadersOnly<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, PerfTestSharedMutexReadersOnlyAbsl) { PerfTestSharedMutexReadersOnly<DB::AbslSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexReadersOnlyStd) { PerfTestSharedMutexReadersOnly<std::shared_mutex>(); }

#ifdef OS_LINUX
TEST(Threading, PerfTestSharedMutexWritersOnlySelf) { PerfTestSharedMutexWritersOnly<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, PerfTestSharedMutexWritersOnlyAbsl) { PerfTestSharedMutexWritersOnly<DB::AbslSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexWritersOnlyStd) { PerfTestSharedMutexWritersOnly<std::shared_mutex>(); }

#ifdef OS_LINUX
TEST(Threading, PerfTestSharedMutexRWSelf) { PerfTestSharedMutexRW<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, PerfTestSharedMutexRWAbsl) { PerfTestSharedMutexRW<DB::AbslSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexRWStd) { PerfTestSharedMutexRW<std::shared_mutex>(); }

