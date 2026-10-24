/// Background threads on top of real arenas (as in `malloc_init_hard`: boot0, the arenas, boot1, then thread 0 for
/// arena 0): the default number of threads (4), asynchronous creation of the other threads by thread 0 when arenas are
/// created, thread naming, the signal mask and the CPU affinity of the threads, the wakeup from the indefinite sleep
/// on new dirty pages and the purging by the background thread, stats, enabling / disabling (stop and join), the
/// `background_thread` and `max_background_threads` mallctl leaves, fork handlers (the child has background threads
/// disabled and can enable them again), and the sleep interval computation.

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/MallctlImpl.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>

using namespace jemalloc;

namespace
{

constinit ThreadState thread_state;

bool waitFor(auto && predicate)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

std::string readFile(const std::string & path)
{
    std::string result;
    FILE * f = std::fopen(path.c_str(), "r");
    if (!f)
        return result;
    char buf[4096];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0)
        result.append(buf, n);
    std::fclose(f);
    return result;
}

/// The `/proc/self/task/<tid>` directories of the threads named "jemalloc_bg_thd".
std::vector<std::string> backgroundThreadTasks()
{
    std::vector<std::string> result;
    DIR * dir = opendir("/proc/self/task");
    if (!dir)
        return result;
    while (dirent * entry = readdir(dir))
    {
        if (entry->d_name[0] == '.')
            continue;
        std::string task = std::string("/proc/self/task/") + entry->d_name;
        if (readFile(task + "/comm") == "jemalloc_bg_thd\n")
            result.push_back(task);
    }
    closedir(dir);
    return result;
}

size_t countBackgroundThreadTasks()
{
    return backgroundThreadTasks().size();
}

std::string statusField(const std::string & task, const char * field)
{
    std::string status = readFile(task + "/status");
    std::string key = std::string(field) + ":\t";
    size_t pos = status.find(key);
    if (pos == std::string::npos)
        return {};
    size_t end = status.find('\n', pos);
    return status.substr(pos + key.size(), end - pos - key.size());
}

uint64_t totalRuns(unsigned idx)
{
    BackgroundThreadInfo * info = &background_thread_info[idx];
    MutexLock lock(&thread_state, info->mutex);
    return info->total_num_runs;
}

BackgroundThreadState threadState(unsigned idx)
{
    BackgroundThreadInfo * info = &background_thread_info[idx];
    MutexLock lock(&thread_state, info->mutex);
    return info->state;
}

/// The thread is waiting (it holds its mutex while working) with the given kind of sleep.
bool sleepsIndefinitely(unsigned idx)
{
    BackgroundThreadInfo * info = &background_thread_info[idx];
    MutexLock lock(&thread_state, info->mutex);
    return info->total_num_runs > 0 && info->indefiniteSleep();
}

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    booted = true;

    /// The ClickHouse configuration: `background_thread:true`, `per_cpu_arena:percpu` (pins the threads to CPUs);
    /// fast decay so that the background threads purge quickly.
    options.background_thread = true;
    options.per_cpu_arena = PerCPUArenaMode::PerCPU;
    options.dirty_decay_ms = 100;
    options.muzzy_decay_ms = 0;

    REQUIRE(!pages::boot());
    sizeBoot(default_size_class_data, options.cache_oblivious);
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_extent_map_global.init(base0Get(), /* zeroed */ true));
    /// `malloc_init_hard_a0_locked`: `background_thread_boot0` after the options.
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!arenaBoot(&default_size_class_data, base0Get(), false));
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));
    /// The background threads use the TLS TSD (`tsd_internal_fetch`).
    REQUIRE(!ThreadStateStorage::boot0());

    num_arenas_auto = 1;
    manual_arena_base = num_arenas_auto + 1;
    a0 = arenaInit(nullptr, 0, &arena_config_default);
    REQUIRE(a0 != nullptr);
    numArenasTotalSet(num_arenas_auto);
    /// The huge arena (index 1) is created before the background threads are enabled.
    if (arenaInitHuge(nullptr, a0))
        numArenasTotalIncrement();
    manual_arena_base = numArenasTotalGet();

    thread_state.state.store(thread_state_nominal_slow, std::memory_order_relaxed);
}

}

TEST(BackgroundThread, SleepInterval)
{
    CHECK_EQ(BACKGROUND_THREAD_MIN_INTERVAL_NS, uint64_t(100000000));
    CHECK_EQ(backgroundThreadSleepInterval(BACKGROUND_THREAD_DEFERRED_MAX), BACKGROUND_THREAD_INDEFINITE_SLEEP);
    CHECK_EQ(backgroundThreadSleepInterval(0), BACKGROUND_THREAD_MIN_INTERVAL_NS);
    CHECK_EQ(backgroundThreadSleepInterval(99999999), BACKGROUND_THREAD_MIN_INTERVAL_NS);
    CHECK_EQ(backgroundThreadSleepInterval(100000000), uint64_t(100000000));
    CHECK_EQ(backgroundThreadSleepInterval(100000001), uint64_t(100000001));
    CHECK_EQ(backgroundThreadSleepInterval(UINT64_MAX - 1), UINT64_MAX - 1);

    /// Thread 1 of 4 over 10 arenas (3 and 9 missing): works on 1, 5 (not 9); stops querying once the minimum is
    /// at most 100 ms.
    struct Ops
    {
        std::vector<uint64_t> times;
        std::vector<unsigned> worked;
        std::vector<unsigned> queried;
        const uint64_t * get(unsigned i) const { return (i == 3 || i == 9) ? nullptr : &times[i]; }
        void doWork(const uint64_t * a) { worked.push_back(unsigned(a - times.data())); }
        uint64_t timeUntilDeferredWork(const uint64_t * a)
        {
            queried.push_back(unsigned(a - times.data()));
            return *a;
        }
    };
    Ops ops;
    ops.times = {0, 500000000, 0, 0, 0, 50000000, 0, 0, 0, 7};
    CHECK_EQ(backgroundWorkPass(1, 10, 4, false, ops), BACKGROUND_THREAD_MIN_INTERVAL_NS);
    REQUIRE(ops.worked.size() == 2);
    CHECK_EQ(ops.worked[0], 1u);
    CHECK_EQ(ops.worked[1], 5u);
    REQUIRE(ops.queried.size() == 2);
    CHECK_EQ(ops.queried[1], 5u);

    /// After an indefinite sleep: no work, only scheduling.
    Ops ops2;
    ops2.times = {0, 300000000, 0, 0, 0, 200000000, 0, 0, 0, 7};
    CHECK_EQ(backgroundWorkPass(1, 10, 4, true, ops2), uint64_t(200000000));
    CHECK(ops2.worked.empty());
    CHECK_EQ(ops2.queried.size(), size_t(2));

    /// Nothing to do anywhere: indefinite.
    Ops ops3;
    ops3.times = std::vector<uint64_t>(10, BACKGROUND_THREAD_DEFERRED_MAX);
    CHECK_EQ(backgroundWorkPass(0, 10, 4, false, ops3), BACKGROUND_THREAD_INDEFINITE_SLEEP);
    CHECK_EQ(ops3.worked.size(), size_t(3));

    /// Once the minimum is <= 100 ms the remaining arenas are worked on but not queried.
    Ops ops4;
    ops4.times = {100000000, 0, 0, 0, 1, 0, 0, 0, 1, 0};
    CHECK_EQ(backgroundWorkPass(0, 10, 4, false, ops4), BACKGROUND_THREAD_MIN_INTERVAL_NS);
    CHECK_EQ(ops4.worked.size(), size_t(3));
    CHECK_EQ(ops4.queried.size(), size_t(1));
}

TEST(BackgroundThread, Boot)
{
    bootOnce();
    CHECK_EQ(options.max_background_threads, MAX_BACKGROUND_THREAD_LIMIT + 1);
    REQUIRE(!backgroundThreadBoot1(nullptr, base0Get()));
    /// The default 4096 is replaced by 4 (DEFAULT_NUM_BACKGROUND_THREAD).
    CHECK_EQ(options.max_background_threads, size_t(4));
    CHECK_EQ(max_background_threads, size_t(4));
    CHECK(backgroundThreadEnabled());
    CHECK_EQ(num_background_threads, size_t(0));
    for (unsigned i = 0; i < 4; ++i)
    {
        CHECK(background_thread_info[i].state == BackgroundThreadState::Stopped);
        CHECK(!background_thread_info[i].indefiniteSleep());
        CHECK_EQ(background_thread_info[i].wakeupTimeGet(), uint64_t(0));
        CHECK_EQ(reinterpret_cast<uintptr_t>(background_thread_info) % CACHE_LINE, uintptr_t(0));
    }
    CHECK(backgroundThreadInfoGet(5) == &background_thread_info[1]);
    CHECK(arenaBackgroundThreadInfoGet(a0) == &background_thread_info[0]);
    CHECK(waitFor([] { return countBackgroundThreadTasks() == 0; }));

    /// Thread `idx % 4` serves arena `idx`, but only arena 0 may create thread 0.
    CHECK(!backgroundThreadCreate(thread_state, 4));
    CHECK_EQ(num_background_threads, size_t(0));
    CHECK(threadState(0) == BackgroundThreadState::Stopped);

    /// As at the end of `malloc_init_hard`.
    backgroundThreadMallctlInit(&thread_state);
    REQUIRE(!backgroundThreadCreate(thread_state, 0));
    CHECK_EQ(num_background_threads, size_t(1));
    CHECK(threadState(0) == BackgroundThreadState::Started);
    /// The first pass does no work, then nothing is pending: an indefinite sleep.
    REQUIRE(waitFor([] { return sleepsIndefinitely(0); }));
    CHECK_EQ(totalRuns(0), uint64_t(1));
    /// The huge arena (1) was created before: its thread is not started.
    CHECK(threadState(1) == BackgroundThreadState::Stopped);
    CHECK_EQ(countBackgroundThreadTasks(), size_t(1));

    /// Creating it again is a no-op.
    REQUIRE(!backgroundThreadCreate(thread_state, 0));
    CHECK_EQ(num_background_threads, size_t(1));
}

TEST(BackgroundThread, ThreadProperties)
{
    auto tasks = backgroundThreadTasks();
    REQUIRE(tasks.size() == 1);
    char name[32] = {};
    CHECK_EQ(pthread_getname_np(background_thread_info[0].thread, name, sizeof(name)), 0);
    CHECK_STREQ(name, "jemalloc_bg_thd");
    /// All signals are blocked (glibc keeps its internal ones unblockable).
    uint64_t blocked = std::stoull(statusField(tasks[0], "SigBlk"), nullptr, 16);
    for (int signal_number : {SIGINT, SIGTERM, SIGHUP, SIGUSR1, SIGUSR2, SIGPIPE, SIGALRM, SIGCHLD, SIGPROF})
        CHECK((blocked >> (signal_number - 1)) & 1);
    /// With per-CPU arenas thread `i` is pinned to CPU `i` (if it exists).
    if (std::thread::hardware_concurrency() > 0)
        CHECK_EQ(statusField(tasks[0], "Cpus_allowed_list"), std::string("0"));
}

TEST(BackgroundThread, AsynchronousCreation)
{
    /// New arenas 2..6: threads 2, 3, 0 (exists), 1, 2 (exists). Thread 0 creates the others when signaled.
    std::vector<Arena *> created;
    for (unsigned i = 0; i < 5; ++i)
    {
        Arena * arena = arenaInit(&thread_state, numArenasTotalGet(), &arena_config_default);
        REQUIRE(arena != nullptr);
        created.push_back(arena);
    }
    CHECK_EQ(numArenasTotalGet(), 7u);
    CHECK_EQ(num_background_threads, size_t(4));
    for (unsigned i = 0; i < 4; ++i)
        CHECK(threadState(i) == BackgroundThreadState::Started);
    REQUIRE(waitFor([] { return countBackgroundThreadTasks() == 4; }));
    for (unsigned i = 0; i < 4; ++i)
        REQUIRE(waitFor([i] { return sleepsIndefinitely(i); }));

    unsigned num_cpus_online = std::thread::hardware_concurrency();
    for (unsigned i = 1; i < 4; ++i)
    {
        char name[32] = {};
        CHECK_EQ(pthread_getname_np(background_thread_info[i].thread, name, sizeof(name)), 0);
        CHECK_STREQ(name, "jemalloc_bg_thd");
    }
    for (const auto & task : backgroundThreadTasks())
    {
        std::string cpus = statusField(task, "Cpus_allowed_list");
        if (num_cpus_online >= 4)
            CHECK(cpus == "0" || cpus == "1" || cpus == "2" || cpus == "3");
    }

    BackgroundThreadStats stats;
    REQUIRE(!backgroundThreadStatsRead(&thread_state, &stats));
    CHECK_EQ(stats.num_threads, size_t(4));
    CHECK_GE(stats.num_runs, uint64_t(4));
}

TEST(BackgroundThread, WakeupAndPurge)
{
    /// Arena 5 is served by thread 1 (which also serves the huge arena 1).
    Arena * arena = arenaGet(&thread_state, 5, false);
    REQUIRE(arena != nullptr);
    CHECK(arenaBackgroundThreadInfoGet(arena) == &background_thread_info[1]);
    REQUIRE(waitFor([] { return sleepsIndefinitely(1); }));
    uint64_t runs_before = totalRuns(1);

    size_t size = 4 << 20;
    void * p = arenaMallocHard(&thread_state, arena, size, size_classes::sizeToIndex(size), false, false);
    REQUIRE(p != nullptr);
    std::memset(p, 1, size);
    Extent * extent = arena_extent_map_global.extentLookup(&thread_state, p);
    REQUIRE(extent != nullptr);
    largeDeallocate(&thread_state, extent);
    CHECK_GT(arena->page_allocator_shard.numDirtyGet(), size_t(0));

    /// The deallocation signals the indefinitely sleeping thread; it reschedules (a finite wakeup) and then purges.
    REQUIRE(waitFor([&] { return arena->page_allocator_shard.numDirtyGet() == 0; }));
    CHECK_GT(totalRuns(1), runs_before + 1);
    REQUIRE(waitFor([] { return sleepsIndefinitely(1); }));
    {
        MutexLock lock(&thread_state, background_thread_info[1].mutex);
        CHECK_EQ(background_thread_info[1].num_pages_to_purge_new, size_t(0));
    }

    /// An early wakeup with less than the minimal interval remaining does nothing; otherwise it signals.
    Nanoseconds remaining = Nanoseconds::fromNanoseconds(BACKGROUND_THREAD_MIN_INTERVAL_NS - 1);
    backgroundThreadWakeupEarly(&background_thread_info[1], &remaining);
    remaining = Nanoseconds::fromNanoseconds(BACKGROUND_THREAD_MIN_INTERVAL_NS);
    uint64_t runs = totalRuns(1);
    backgroundThreadWakeupEarly(&background_thread_info[1], &remaining);
    REQUIRE(waitFor([&] { return totalRuns(1) > runs; }));

    BackgroundThreadStats stats;
    REQUIRE(!backgroundThreadStatsRead(&thread_state, &stats));
    CHECK_EQ(stats.num_threads, size_t(4));
    CHECK_GT(stats.run_interval.ns(), uint64_t(0));
}

TEST(BackgroundThread, DisableEnable)
{
    {
        MutexLock lock(&thread_state, background_thread_lock);
        backgroundThreadEnabledSet(&thread_state, false);
        REQUIRE(!backgroundThreadsDisable(thread_state));
    }
    CHECK_EQ(num_background_threads, size_t(0));
    for (unsigned i = 0; i < 4; ++i)
    {
        CHECK(threadState(i) == BackgroundThreadState::Stopped);
        CHECK_EQ(background_thread_info[i].wakeupTimeGet(), uint64_t(0));
    }
    /// Joined.
    CHECK(waitFor([] { return countBackgroundThreadTasks() == 0; }));
    BackgroundThreadStats stats;
    CHECK(backgroundThreadStatsRead(&thread_state, &stats));
    /// Arena creation does not start threads while disabled.
    CHECK(!backgroundThreadCreate(thread_state, 2));
    CHECK_EQ(num_background_threads, size_t(0));

    {
        MutexLock lock(&thread_state, background_thread_lock);
        backgroundThreadEnabledSet(&thread_state, true);
        REQUIRE(!backgroundThreadsEnable(thread_state));
    }
    /// All threads with an existing arena are marked at once.
    CHECK_EQ(num_background_threads, size_t(4));
    REQUIRE(waitFor([] { return countBackgroundThreadTasks() == 4; }));
    for (unsigned i = 0; i < 4; ++i)
        REQUIRE(waitFor([i] { return sleepsIndefinitely(i); }));
    /// The stats restart from zero.
    CHECK_EQ(totalRuns(1), uint64_t(1));
}

TEST(BackgroundThread, Mallctl)
{
    bool enabled = false;
    size_t len = sizeof(enabled);
    CHECK_EQ(mallctl::backgroundThread(thread_state, nullptr, 0, &enabled, &len, nullptr, 0), 0);
    CHECK(enabled);

    size_t max = 0;
    len = sizeof(max);
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, &max, &len, nullptr, 0), 0);
    CHECK_EQ(max, size_t(4));

    /// Wrong sizes, out-of-range values.
    size_t new_max = 2;
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, nullptr, nullptr, &new_max, sizeof(unsigned)), EINVAL);
    new_max = 0;
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, nullptr, nullptr, &new_max, sizeof(new_max)), EINVAL);
    new_max = 5;
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, nullptr, nullptr, &new_max, sizeof(new_max)), EINVAL);
    new_max = 4;
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, nullptr, nullptr, &new_max, sizeof(new_max)), 0);
    /// A short old buffer: EINVAL without changing anything.
    unsigned short_old = 0;
    len = sizeof(short_old);
    new_max = 2;
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, &short_old, &len, &new_max, sizeof(new_max)), EINVAL);
    CHECK_EQ(max_background_threads, size_t(4));
    bool b = false;
    CHECK_EQ(mallctl::backgroundThread(thread_state, nullptr, 0, nullptr, nullptr, &b, sizeof(int)), EINVAL);
    CHECK(backgroundThreadEnabled());

    /// Restart with 2 threads: arena `i` is now served by thread `i % 2`.
    len = sizeof(max);
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, &max, &len, &new_max, sizeof(new_max)), 0);
    CHECK_EQ(max, size_t(4));
    CHECK_EQ(max_background_threads, size_t(2));
    CHECK_EQ(options.max_background_threads, size_t(4));
    CHECK_EQ(num_background_threads, size_t(2));
    REQUIRE(waitFor([] { return countBackgroundThreadTasks() == 2; }));
    CHECK(arenaBackgroundThreadInfoGet(arenaGet(&thread_state, 5, false)) == &background_thread_info[1]);
    CHECK(arenaBackgroundThreadInfoGet(arenaGet(&thread_state, 6, false)) == &background_thread_info[0]);
    CHECK(threadState(2) == BackgroundThreadState::Stopped);

    /// Stop and join through the mallctl.
    b = false;
    enabled = true;
    len = sizeof(enabled);
    CHECK_EQ(mallctl::backgroundThread(thread_state, nullptr, 0, &enabled, &len, &b, sizeof(b)), 0);
    CHECK(enabled);
    CHECK(!backgroundThreadEnabled());
    CHECK_EQ(num_background_threads, size_t(0));
    CHECK(waitFor([] { return countBackgroundThreadTasks() == 0; }));
    /// Same value: nothing happens.
    CHECK_EQ(mallctl::backgroundThread(thread_state, nullptr, 0, nullptr, nullptr, &b, sizeof(b)), 0);

    /// While disabled, `max_background_threads` is just assigned.
    new_max = 3;
    CHECK_EQ(mallctl::maxBackgroundThreads(thread_state, nullptr, 0, nullptr, nullptr, &new_max, sizeof(new_max)), 0);
    CHECK_EQ(max_background_threads, size_t(3));
    CHECK_EQ(num_background_threads, size_t(0));

    b = true;
    CHECK_EQ(mallctl::backgroundThread(thread_state, nullptr, 0, nullptr, nullptr, &b, sizeof(b)), 0);
    CHECK(backgroundThreadEnabled());
    CHECK_EQ(num_background_threads, size_t(3));
    REQUIRE(waitFor([] { return countBackgroundThreadTasks() == 3; }));
    for (unsigned i = 0; i < 3; ++i)
        REQUIRE(waitFor([i] { return sleepsIndefinitely(i); }));
}

TEST(BackgroundThread, Fork)
{
    backgroundThreadPrefork0(&thread_state);
    backgroundThreadPrefork1(&thread_state);
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0)
    {
        backgroundThreadPostforkChild(&thread_state);
        int code = 0;
        if (backgroundThreadEnabled())
            code |= 1;
        if (num_background_threads != 0)
            code |= 2;
        for (unsigned i = 0; i < max_background_threads; ++i)
        {
            if (background_thread_info[i].state != BackgroundThreadState::Stopped)
                code |= 4;
            if (background_thread_info[i].wakeupTimeGet() != 0)
                code |= 4;
        }
        if (countBackgroundThreadTasks() != 0)
            code |= 8;
        /// The child can start its own threads (the condition variables were re-created).
        bool b = true;
        if (mallctl::backgroundThread(thread_state, nullptr, 0, nullptr, nullptr, &b, sizeof(b)) != 0)
            code |= 16;
        if (!waitFor([] { return countBackgroundThreadTasks() == 3; }))
            code |= 32;
        if (!waitFor([] { return sleepsIndefinitely(0); }))
            code |= 32;
        b = false;
        /// `pthread_join` returns when the kernel clears the thread id, which may happen slightly before the task
        /// disappears from /proc/self/task, so wait for it.
        if (mallctl::backgroundThread(thread_state, nullptr, 0, nullptr, nullptr, &b, sizeof(b)) != 0
            || !waitFor([] { return countBackgroundThreadTasks() == 0; }))
            code |= 64;
        _exit(code);
    }
    backgroundThreadPostforkParent(&thread_state);
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    REQUIRE(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 0);

    /// The parent still has its threads.
    CHECK(backgroundThreadEnabled());
    CHECK_EQ(num_background_threads, size_t(3));
    CHECK_EQ(countBackgroundThreadTasks(), size_t(3));

    /// Leave no threads behind.
    bool b = false;
    CHECK_EQ(mallctl::backgroundThread(thread_state, nullptr, 0, nullptr, nullptr, &b, sizeof(b)), 0);
    CHECK(waitFor([] { return countBackgroundThreadTasks() == 0; }));
}
