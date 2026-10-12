/// Tests of `Mutex`: correctness under contention, the profiling counters and fork handling.

#include <allocator/Mutex.h>

#include "Test.h"

#include <atomic>
#include <thread>
#include <unistd.h>
#include <sys/wait.h>

using namespace jemalloc;

namespace
{

constinit Mutex global_mutex;

ThreadState * fakeThreadState(uintptr_t i)
{
    return reinterpret_cast<ThreadState *>(0x1000 * (i + 1));
}

/// Holds `mutex` while another thread runs `lock`, waits until that thread is blocked in the slow path, then
/// releases it.
void contendOnce(Mutex & mutex)
{
    mutex.lock(fakeThreadState(0));
    std::thread waiter(
        [&]
        {
            mutex.lock(fakeThreadState(1));
            mutex.unlock(fakeThreadState(1));
        });
    while (mutex.profilingData().num_waiting_threads.load(std::memory_order_relaxed) == 0)
        std::this_thread::yield();
    /// The waiter makes one last `trylock` after incrementing `num_waiting_threads`; give it time to get past it and block
    /// (if it does not, it acquires the lock through that `trylock`, which the checks allow).
    Nanoseconds start = Nanoseconds::now();
    while (start.nsSince() < 20 * Nanoseconds::MILLION)
        std::this_thread::yield();
    mutex.unlock(fakeThreadState(0));
    waiter.join();
}

}

TEST(Mutex, Layout)
{
    static_assert(sizeof(MutexProfilingData) == 64);
    static_assert(offsetof(MutexProfilingData, num_waiting_threads) == 36);
    static_assert(offsetof(MutexProfilingData, prev_owner) == 48);
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
    static_assert(sizeof(Mutex) == 120);
#endif
    CHECK_EQ(sizeof(Mutex), size_t(72) + sizeof(pthread_mutex_t));
}

TEST(Mutex, Names)
{
    CHECK_STREQ(mutex_profiling_global_names[global_profiling_mutex_mallctl], "ctl");
    CHECK_STREQ(mutex_profiling_global_names[mutex_profiling_num_global_mutexes - 1], "prof_stats");
    CHECK_STREQ(mutex_profiling_arena_names[arena_profiling_mutex_base], "base");
    CHECK_STREQ(mutex_profiling_arena_names[mutex_profiling_num_arena_mutexes - 1], "hpa_sec");
    CHECK_EQ(unsigned(mutex_profiling_num_global_mutexes), 9u);
    CHECK_EQ(unsigned(mutex_profiling_num_arena_mutexes), 12u);
    CHECK_STREQ(mutex_profiling_uint64_counters[mutex_counter_total_wait_time].human, "total_wait_ns");
    CHECK(mutex_profiling_uint64_counters[mutex_counter_num_spin_acquired_per_second].derived);
    CHECK_EQ(
        mutex_profiling_uint64_counters[mutex_counter_num_spin_acquired_per_second].base_counter,
        unsigned(mutex_counter_num_spin_acquired));
    CHECK_STREQ(mutex_profiling_uint32_counters[mutex_counter_max_num_threads].human, "max_n_thds");
    CHECK_EQ(unsigned(MutexRank::BACKGROUND_THREAD), 13u);
    CHECK_EQ(unsigned(MutexRank::ARENA_LARGE), 24u);
    CHECK_EQ(unsigned(MutexRank::BIN), 0x1000u);
    CHECK_EQ(options.mutex_max_spin, int64_t(600));
}

TEST(Mutex, StaticInitializer)
{
    global_mutex.lock(fakeThreadState(0));
    CHECK(global_mutex.isLocked());
    global_mutex.unlock(fakeThreadState(0));
    CHECK(!global_mutex.isLocked());
    CHECK_EQ(global_mutex.profilingData().num_lock_ops, 1u);
}

TEST(Mutex, OwnerSwitches)
{
    Mutex mutex;
    REQUIRE(!mutex.init("test", MutexRank::LEAF));
    for (uintptr_t i = 0; i < 10; ++i)
    {
        mutex.lock(fakeThreadState(i / 3));
        mutex.unlock(fakeThreadState(i / 3));
    }
    mutex.lock(nullptr);
    MutexProfilingData data;
    mutex.profilingRead(nullptr, data);
    mutex.unlock(nullptr);
    /// 10 + the read lock.
    CHECK_EQ(data.num_lock_ops, 11u);
    /// i / 3 takes 4 distinct values, then nullptr.
    CHECK_EQ(data.num_owner_switches, 5u);
    CHECK(data.prev_owner == nullptr);
    CHECK_EQ(data.num_wait_times, 0u);
    CHECK_EQ(data.num_spin_acquired, 0u);
}

TEST(Mutex, TryLock)
{
    Mutex mutex;
    REQUIRE(!mutex.init("test", MutexRank::LEAF));
    CHECK(mutex.tryLock(fakeThreadState(0)));
    bool other_result = true;
    std::thread([&] { other_result = mutex.tryLock(fakeThreadState(1)); }).join();
    CHECK(!other_result);
    mutex.unlock(fakeThreadState(0));
    std::thread(
        [&]
        {
            other_result = mutex.tryLock(fakeThreadState(1));
            if (other_result)
                mutex.unlock(fakeThreadState(1));
        })
        .join();
    CHECK(other_result);
    /// The failed trylock is not counted.
    CHECK_EQ(mutex.profilingData().num_lock_ops, 2u);
    CHECK_EQ(mutex.profilingData().num_owner_switches, 2u);
}

TEST(Mutex, Contention)
{
    unsigned saved_num_cpus = num_cpus;
    num_cpus = 8;
    Mutex mutex;
    REQUIRE(!mutex.init("test", MutexRank::LEAF));
    constexpr int num_threads = 8;
    constexpr int iterations = 100000;
    uint64_t counter = 0;
    std::thread threads[num_threads];
    for (int t = 0; t < num_threads; ++t)
        threads[t] = std::thread(
            [&, t]
            {
                for (int i = 0; i < iterations; ++i)
                {
                    MutexLock lock(fakeThreadState(t), mutex);
                    ++counter;
                }
            });
    for (auto & thread : threads)
        thread.join();

    CHECK_EQ(counter, uint64_t(num_threads) * iterations);
    const MutexProfilingData & data = mutex.profilingData();
    CHECK_EQ(data.num_lock_ops, uint64_t(num_threads) * iterations);
    CHECK_LE(data.num_spin_acquired + data.num_wait_times, data.num_lock_ops);
    CHECK_LE(data.num_owner_switches, data.num_lock_ops);
    CHECK_GE(data.num_owner_switches, uint64_t(num_threads));
    CHECK_LE(data.max_num_threads, uint32_t(num_threads));
    CHECK_EQ(data.num_waiting_threads.load(), 0u);
    CHECK_LE(data.max_wait_time.ns(), data.total_wait_time.ns());
    CHECK(!mutex.isLocked());
    num_cpus = saved_num_cpus;
}

TEST(Mutex, BlockingPath)
{
    unsigned saved_num_cpus = num_cpus;
    int64_t saved_spin = options.mutex_max_spin;

    for (unsigned cpus : {1u, 4u})
    {
        num_cpus = cpus;
        options.mutex_max_spin = 0;
        Mutex mutex;
        REQUIRE(!mutex.init("test", MutexRank::LEAF));
        contendOnce(mutex);
        contendOnce(mutex);

        const MutexProfilingData & data = mutex.profilingData();
        CHECK_EQ(data.num_lock_ops, 4u);
        CHECK_EQ(data.num_owner_switches, 4u);
        /// Every contended lock ends either blocking (n_wait_times) or in the last trylock (n_spin_acquired).
        CHECK_EQ(data.num_wait_times + data.num_spin_acquired, 2u);
        CHECK_GE(data.num_wait_times, 1u);
        CHECK_EQ(data.max_num_threads, 1u);
        CHECK_GT(data.total_wait_time.ns(), 0u);
        CHECK_LE(data.max_wait_time.ns(), data.total_wait_time.ns());
        CHECK_GE(2 * data.max_wait_time.ns(), data.total_wait_time.ns());

        /// Reset.
        mutex.lock(nullptr);
        mutex.profilingDataReset(nullptr);
        mutex.unlock(nullptr);
        /// The reset happens after the lock was counted.
        CHECK_EQ(mutex.profilingData().num_lock_ops, 0u);
        CHECK_EQ(mutex.profilingData().num_wait_times, 0u);
        CHECK(mutex.profilingData().prev_owner == nullptr);
        CHECK(mutex.profilingData().total_wait_time.equalsZero());
    }

    num_cpus = saved_num_cpus;
    options.mutex_max_spin = saved_spin;
}

TEST(Mutex, ProfilingAggregation)
{
    unsigned saved_num_cpus = num_cpus;
    int64_t saved_spin = options.mutex_max_spin;
    num_cpus = 2;
    options.mutex_max_spin = 0;

    Mutex a;
    Mutex b;
    REQUIRE(!a.init("a", MutexRank::LEAF));
    REQUIRE(!b.init("b", MutexRank::LEAF));
    contendOnce(a);
    for (int i = 0; i < 5; ++i)
    {
        b.lock(fakeThreadState(7));
        b.unlock(fakeThreadState(7));
    }

    MutexProfilingData accumulated;
    a.lock(fakeThreadState(0));
    a.profilingAccumulated(fakeThreadState(0), accumulated);
    a.unlock(fakeThreadState(0));
    b.lock(fakeThreadState(7));
    b.profilingAccumulated(fakeThreadState(7), accumulated);
    b.unlock(fakeThreadState(7));
    /// a: 2 + 1, b: 5 + 1.
    CHECK_EQ(accumulated.num_lock_ops, 3u + 6u);
    CHECK_EQ(accumulated.num_owner_switches, 3u + 1u);
    CHECK_EQ(accumulated.num_wait_times + accumulated.num_spin_acquired, 1u);
    CHECK_EQ(accumulated.max_num_threads, 1u);
    CHECK(accumulated.prev_owner == nullptr);
    CHECK_EQ(accumulated.max_wait_time.ns(), a.profilingData().max_wait_time.ns());

    MutexProfilingData max;
    b.lock(nullptr);
    b.profilingMaxUpdate(nullptr, max);
    b.unlock(nullptr);
    a.lock(nullptr);
    a.profilingMaxUpdate(nullptr, max);
    a.unlock(nullptr);
    CHECK_EQ(max.num_lock_ops, 7u);
    CHECK_EQ(max.num_owner_switches, 4u);
    CHECK_EQ(max.num_wait_times + max.num_spin_acquired, 1u);

    MutexProfilingData sum;
    MutexProfilingData read;
    a.lock(nullptr);
    a.profilingRead(nullptr, read);
    a.unlock(nullptr);
    CHECK(read.prev_owner == nullptr);
    read.num_waiting_threads.store(3);
    sum.merge(read);
    sum.merge(read);
    CHECK_EQ(sum.num_lock_ops, 2 * read.num_lock_ops);
    CHECK_EQ(sum.num_waiting_threads.load(), 6u);
    CHECK_EQ(sum.total_wait_time.ns(), 2 * read.total_wait_time.ns());
    CHECK_EQ(sum.max_wait_time.ns(), read.max_wait_time.ns());

    MutexProfilingData copy;
    copy.copyFrom(read);
    CHECK_EQ(copy.num_lock_ops, read.num_lock_ops);
    CHECK_EQ(copy.num_waiting_threads.load(), 0u);

    num_cpus = saved_num_cpus;
    options.mutex_max_spin = saved_spin;
}

TEST(Mutex, Fork)
{
    Mutex mutex;
    REQUIRE(!mutex.init("test", MutexRank::LEAF));
    mutex.prefork(nullptr);
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0)
    {
        mutex.postforkChild(nullptr);
        /// Like jemalloc, re-initialization resets the counters but not the `locked` hint.
        bool ok = mutex.isLocked() && mutex.profilingData().num_lock_ops == 0 && mutex.tryLock(nullptr)
            && mutex.profilingData().num_lock_ops == 1;
        mutex.unlock(nullptr);
        _exit(ok ? 0 : 1);
    }
    mutex.postforkParent(nullptr);
    CHECK(!mutex.isLocked());
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 0);
}
