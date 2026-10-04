/// Tests of `Mutex`: correctness under contention, the profiling counters and fork handling.

#include <allocator/Mutex.h>

#include "Test.h"

#include <atomic>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>

using namespace jemalloc;

namespace
{

constinit Mutex global_mutex;

ThreadState * fakeTsd(uintptr_t i)
{
    return reinterpret_cast<ThreadState *>(0x1000 * (i + 1));
}

/// Holds `mutex` while another thread runs `lock`, waits until that thread is blocked in the slow path, then
/// releases it.
void contendOnce(Mutex & mutex)
{
    mutex.lock(fakeTsd(0));
    std::thread waiter(
        [&]
        {
            mutex.lock(fakeTsd(1));
            mutex.unlock(fakeTsd(1));
        });
    while (mutex.profData().n_waiting_thds.load(std::memory_order_relaxed) == 0)
        std::this_thread::yield();
    /// The waiter makes one last `trylock` after incrementing `n_waiting_thds`; give it time to get past it and block
    /// (if it does not, it acquires the lock through that `trylock`, which the checks allow).
    NsTime start = NsTime::now();
    while (start.nsSince() < 20 * NsTime::MILLION)
        std::this_thread::yield();
    mutex.unlock(fakeTsd(0));
    waiter.join();
}

}

TEST(Mutex, Layout)
{
    static_assert(sizeof(MutexProfData) == 64);
    static_assert(offsetof(MutexProfData, n_waiting_thds) == 36);
    static_assert(offsetof(MutexProfData, prev_owner) == 48);
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
    static_assert(sizeof(Mutex) == 120);
#endif
    CHECK_EQ(sizeof(Mutex), size_t(72) + sizeof(pthread_mutex_t));
}

TEST(Mutex, Names)
{
    CHECK_STREQ(mutex_prof_global_names[global_prof_mutex_ctl], "ctl");
    CHECK_STREQ(mutex_prof_global_names[mutex_prof_num_global_mutexes - 1], "prof_stats");
    CHECK_STREQ(mutex_prof_arena_names[arena_prof_mutex_base], "base");
    CHECK_STREQ(mutex_prof_arena_names[mutex_prof_num_arena_mutexes - 1], "hpa_sec");
    CHECK_EQ(unsigned(mutex_prof_num_global_mutexes), 9u);
    CHECK_EQ(unsigned(mutex_prof_num_arena_mutexes), 12u);
    CHECK_STREQ(mutex_prof_uint64_counters[mutex_counter_total_wait_time].human, "total_wait_ns");
    CHECK(mutex_prof_uint64_counters[mutex_counter_num_spin_acq_ps].derived);
    CHECK_EQ(mutex_prof_uint64_counters[mutex_counter_num_spin_acq_ps].base_counter, unsigned(mutex_counter_num_spin_acq));
    CHECK_STREQ(mutex_prof_uint32_counters[mutex_counter_max_num_thds].human, "max_n_thds");
    CHECK_EQ(unsigned(MutexRank::BACKGROUND_THREAD), 13u);
    CHECK_EQ(unsigned(MutexRank::ARENA_LARGE), 24u);
    CHECK_EQ(unsigned(MutexRank::BIN), 0x1000u);
    CHECK_EQ(opt.mutex_max_spin, int64_t(600));
}

TEST(Mutex, StaticInitializer)
{
    global_mutex.lock(fakeTsd(0));
    CHECK(global_mutex.isLocked());
    global_mutex.unlock(fakeTsd(0));
    CHECK(!global_mutex.isLocked());
    CHECK_EQ(global_mutex.profData().n_lock_ops, 1u);
}

TEST(Mutex, OwnerSwitches)
{
    Mutex mutex;
    REQUIRE(!mutex.init("test", MutexRank::LEAF));
    for (uintptr_t i = 0; i < 10; ++i)
    {
        mutex.lock(fakeTsd(i / 3));
        mutex.unlock(fakeTsd(i / 3));
    }
    mutex.lock(nullptr);
    MutexProfData data;
    mutex.profRead(nullptr, data);
    mutex.unlock(nullptr);
    /// 10 + the read lock.
    CHECK_EQ(data.n_lock_ops, 11u);
    /// i / 3 takes 4 distinct values, then nullptr.
    CHECK_EQ(data.n_owner_switches, 5u);
    CHECK(data.prev_owner == nullptr);
    CHECK_EQ(data.n_wait_times, 0u);
    CHECK_EQ(data.n_spin_acquired, 0u);
}

TEST(Mutex, TryLock)
{
    Mutex mutex;
    REQUIRE(!mutex.init("test", MutexRank::LEAF));
    CHECK(mutex.tryLock(fakeTsd(0)));
    bool other_result = true;
    std::thread([&] { other_result = mutex.tryLock(fakeTsd(1)); }).join();
    CHECK(!other_result);
    mutex.unlock(fakeTsd(0));
    std::thread(
        [&]
        {
            other_result = mutex.tryLock(fakeTsd(1));
            if (other_result)
                mutex.unlock(fakeTsd(1));
        })
        .join();
    CHECK(other_result);
    /// The failed trylock is not counted.
    CHECK_EQ(mutex.profData().n_lock_ops, 2u);
    CHECK_EQ(mutex.profData().n_owner_switches, 2u);
}

TEST(Mutex, Contention)
{
    unsigned saved_ncpus = ncpus;
    ncpus = 8;
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
                    MutexLock lock(fakeTsd(t), mutex);
                    ++counter;
                }
            });
    for (auto & thread : threads)
        thread.join();

    CHECK_EQ(counter, uint64_t(num_threads) * iterations);
    const MutexProfData & data = mutex.profData();
    CHECK_EQ(data.n_lock_ops, uint64_t(num_threads) * iterations);
    CHECK_LE(data.n_spin_acquired + data.n_wait_times, data.n_lock_ops);
    CHECK_LE(data.n_owner_switches, data.n_lock_ops);
    CHECK_GE(data.n_owner_switches, uint64_t(num_threads));
    CHECK_LE(data.max_n_thds, uint32_t(num_threads));
    CHECK_EQ(data.n_waiting_thds.load(), 0u);
    CHECK_LE(data.max_wait_time.ns(), data.tot_wait_time.ns());
    CHECK(!mutex.isLocked());
    ncpus = saved_ncpus;
}

TEST(Mutex, BlockingPath)
{
    unsigned saved_ncpus = ncpus;
    int64_t saved_spin = opt.mutex_max_spin;

    for (unsigned cpus : {1u, 4u})
    {
        ncpus = cpus;
        opt.mutex_max_spin = 0;
        Mutex mutex;
        REQUIRE(!mutex.init("test", MutexRank::LEAF));
        contendOnce(mutex);
        contendOnce(mutex);

        const MutexProfData & data = mutex.profData();
        CHECK_EQ(data.n_lock_ops, 4u);
        CHECK_EQ(data.n_owner_switches, 4u);
        /// Every contended lock ends either blocking (n_wait_times) or in the last trylock (n_spin_acquired).
        CHECK_EQ(data.n_wait_times + data.n_spin_acquired, 2u);
        CHECK_GE(data.n_wait_times, 1u);
        CHECK_EQ(data.max_n_thds, 1u);
        CHECK_GT(data.tot_wait_time.ns(), 0u);
        CHECK_LE(data.max_wait_time.ns(), data.tot_wait_time.ns());
        CHECK_GE(2 * data.max_wait_time.ns(), data.tot_wait_time.ns());

        /// Reset.
        mutex.lock(nullptr);
        mutex.profDataReset(nullptr);
        mutex.unlock(nullptr);
        /// The reset happens after the lock was counted.
        CHECK_EQ(mutex.profData().n_lock_ops, 0u);
        CHECK_EQ(mutex.profData().n_wait_times, 0u);
        CHECK(mutex.profData().prev_owner == nullptr);
        CHECK(mutex.profData().tot_wait_time.equalsZero());
    }

    ncpus = saved_ncpus;
    opt.mutex_max_spin = saved_spin;
}

TEST(Mutex, ProfAggregation)
{
    unsigned saved_ncpus = ncpus;
    int64_t saved_spin = opt.mutex_max_spin;
    ncpus = 2;
    opt.mutex_max_spin = 0;

    Mutex a;
    Mutex b;
    REQUIRE(!a.init("a", MutexRank::LEAF));
    REQUIRE(!b.init("b", MutexRank::LEAF));
    contendOnce(a);
    for (int i = 0; i < 5; ++i)
    {
        b.lock(fakeTsd(7));
        b.unlock(fakeTsd(7));
    }

    MutexProfData accum;
    a.lock(fakeTsd(0));
    a.profAccum(fakeTsd(0), accum);
    a.unlock(fakeTsd(0));
    b.lock(fakeTsd(7));
    b.profAccum(fakeTsd(7), accum);
    b.unlock(fakeTsd(7));
    /// a: 2 + 1, b: 5 + 1.
    CHECK_EQ(accum.n_lock_ops, 3u + 6u);
    CHECK_EQ(accum.n_owner_switches, 3u + 1u);
    CHECK_EQ(accum.n_wait_times + accum.n_spin_acquired, 1u);
    CHECK_EQ(accum.max_n_thds, 1u);
    CHECK(accum.prev_owner == nullptr);
    CHECK_EQ(accum.max_wait_time.ns(), a.profData().max_wait_time.ns());

    MutexProfData max;
    b.lock(nullptr);
    b.profMaxUpdate(nullptr, max);
    b.unlock(nullptr);
    a.lock(nullptr);
    a.profMaxUpdate(nullptr, max);
    a.unlock(nullptr);
    CHECK_EQ(max.n_lock_ops, 7u);
    CHECK_EQ(max.n_owner_switches, 4u);
    CHECK_EQ(max.n_wait_times + max.n_spin_acquired, 1u);

    MutexProfData sum;
    MutexProfData read;
    a.lock(nullptr);
    a.profRead(nullptr, read);
    a.unlock(nullptr);
    CHECK(read.prev_owner == nullptr);
    read.n_waiting_thds.store(3);
    sum.merge(read);
    sum.merge(read);
    CHECK_EQ(sum.n_lock_ops, 2 * read.n_lock_ops);
    CHECK_EQ(sum.n_waiting_thds.load(), 6u);
    CHECK_EQ(sum.tot_wait_time.ns(), 2 * read.tot_wait_time.ns());
    CHECK_EQ(sum.max_wait_time.ns(), read.max_wait_time.ns());

    MutexProfData copy;
    copy.copyFrom(read);
    CHECK_EQ(copy.n_lock_ops, read.n_lock_ops);
    CHECK_EQ(copy.n_waiting_thds.load(), 0u);

    ncpus = saved_ncpus;
    opt.mutex_max_spin = saved_spin;
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
        bool ok = mutex.isLocked() && mutex.profData().n_lock_ops == 0 && mutex.tryLock(nullptr) && mutex.profData().n_lock_ops == 1;
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
