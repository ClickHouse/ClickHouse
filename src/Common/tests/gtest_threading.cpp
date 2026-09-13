#include <gtest/gtest.h>

#include <thread>
#include <condition_variable>
#include <shared_mutex>
#include <barrier>
#include <atomic>

#include <Common/Exception.h>
#include <Common/SharedMutex.h>
#include <Common/ReadMostlySharedMutex.h>
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

        double ns = static_cast<double>(watch.elapsedNanoseconds());
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

        double ns = static_cast<double>(watch.elapsedNanoseconds());
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

        double ns = static_cast<double>(watch.elapsedNanoseconds());
        std::cout << "thrs = " << thrs << ":\t" << ns / requests << " ns\t" << requests * 1e9 / ns << " rps" << std::endl;
    }
}

// static_assert(sizeof(DB::SelfSharedMutex) == 16);

#ifdef OS_LINUX
TEST(Threading, SharedMutexSmokeSelf) { TestSharedMutex<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, SharedMutexSmokeReadMostly) { TestSharedMutex<DB::ReadMostlySharedMutex>(); }
TEST(Threading, SharedMutexSmokeAbsl) { TestSharedMutex<DB::AbslSharedMutex>(); }
TEST(Threading, SharedMutexSmokeStd) { TestSharedMutex<std::shared_mutex>(); }

#ifdef OS_LINUX
TEST(Threading, PerfTestSharedMutexReadersOnlySelf) { PerfTestSharedMutexReadersOnly<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, PerfTestSharedMutexReadersOnlyReadMostly) { PerfTestSharedMutexReadersOnly<DB::ReadMostlySharedMutex>(); }
TEST(Threading, PerfTestSharedMutexReadersOnlyAbsl) { PerfTestSharedMutexReadersOnly<DB::AbslSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexReadersOnlyStd) { PerfTestSharedMutexReadersOnly<std::shared_mutex>(); }

#ifdef OS_LINUX
TEST(Threading, PerfTestSharedMutexWritersOnlySelf) { PerfTestSharedMutexWritersOnly<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, PerfTestSharedMutexWritersOnlyReadMostly) { PerfTestSharedMutexWritersOnly<DB::ReadMostlySharedMutex>(); }
TEST(Threading, PerfTestSharedMutexWritersOnlyAbsl) { PerfTestSharedMutexWritersOnly<DB::AbslSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexWritersOnlyStd) { PerfTestSharedMutexWritersOnly<std::shared_mutex>(); }

#ifdef OS_LINUX
TEST(Threading, PerfTestSharedMutexRWSelf) { PerfTestSharedMutexRW<DB::SelfSharedMutex>(); }
#endif
TEST(Threading, PerfTestSharedMutexRWReadMostly) { PerfTestSharedMutexRW<DB::ReadMostlySharedMutex>(); }
TEST(Threading, PerfTestSharedMutexRWAbsl) { PerfTestSharedMutexRW<DB::AbslSharedMutex>(); }
TEST(Threading, PerfTestSharedMutexRWStd) { PerfTestSharedMutexRW<std::shared_mutex>(); }


/// ReadMostlySharedMutex publishes a reader with an unconditional fetch_add and
/// only then checks for a writer, while a writer publishes itself and only then
/// waits for readers to drain. Getting the order or the memory ordering of
/// those four operations wrong lets a writer run beside a reader without any
/// single operation looking wrong, so it has to be caught by readers and
/// writers hammering the same state together:
///
///   * `counter` is deliberately a plain size_t. If two writers ever overlap,
///     the increments are lost and the final total does not match.
///   * writers assert no reader is inside while they hold the lock.
///   * readers assert `counter` does not move underneath them, which is the
///     same violation seen from the other side.
TEST(Threading, ReadMostlySharedMutexStressReadersAndWriters)
{
    DB::ReadMostlySharedMutex sm;

    size_t counter = 0; /// guarded by sm; not atomic on purpose
    std::atomic<size_t> writes_done{0};
    std::atomic<int> readers_inside{0};
    std::atomic<bool> saw_reader_during_write{false};
    std::atomic<bool> saw_write_during_read{false};
    std::atomic<bool> stop{false};

    constexpr int num_readers = 16;
    constexpr int num_writers = 4;
    constexpr size_t writes_per_writer = 2000;

    std::vector<std::thread> threads;
    threads.reserve(num_readers + num_writers);

    for (int i = 0; i < num_writers; ++i)
    {
        threads.emplace_back([&]
        {
            for (size_t n = 0; n < writes_per_writer; ++n)
            {
                std::unique_lock lock(sm);
                if (readers_inside.load(std::memory_order_acquire) != 0)
                    saw_reader_during_write.store(true, std::memory_order_release);
                ++counter;
                writes_done.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (int i = 0; i < num_readers; ++i)
    {
        threads.emplace_back([&]
        {
            while (!stop.load(std::memory_order_acquire))
            {
                std::shared_lock lock(sm);
                readers_inside.fetch_add(1, std::memory_order_acq_rel);
                const size_t before = counter;
                /// Give a writer a chance to break in, if it can.
                for (int spin = 0; spin < 64; ++spin)
                    std::atomic_thread_fence(std::memory_order_seq_cst);
                if (counter != before)
                    saw_write_during_read.store(true, std::memory_order_release);
                readers_inside.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }

    for (int i = 0; i < num_writers; ++i)
        threads[i].join();
    stop.store(true, std::memory_order_release);
    for (size_t i = num_writers; i < threads.size(); ++i)
        threads[i].join();

    ASSERT_EQ(writes_done.load(), size_t(num_writers) * writes_per_writer);
    ASSERT_EQ(counter, writes_done.load()) << "lost updates: writers were not mutually exclusive";
    ASSERT_FALSE(saw_reader_during_write.load()) << "a writer held the lock while a reader was inside";
    ASSERT_FALSE(saw_write_during_read.load()) << "a writer modified guarded state while a reader held it";
}
