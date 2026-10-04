/// The TSD life cycle (jemalloc: `tsd.c`): boot, the state machine (uninitialized, nominal, nominal_slow, recompute,
/// minimal_initialized, purgatory, reincarnated), the global slow counter, reentrancy, thread exit cleanup (order of
/// the hooks, destructor rounds, reincarnation), fork, and fiber safety of the TLS access (swapcontext between
/// threads, like the fork's `test/integration/tcache_fiber_migration.c`).

#include <allocator/Options.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include "Test.h"
#include "ThreadTestHooks.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>
#include <sys/wait.h>
#include <ucontext.h>
#include <unistd.h>

using namespace jemalloc;

namespace
{

std::vector<std::string> names(const std::vector<thread_test::HookCall> & log)
{
    std::vector<std::string> result;
    for (const auto & call : log)
        result.push_back(call.name);
    return result;
}

void boot()
{
    static bool booted_once = false;
    if (booted_once)
        return;
    booted_once = true;
    CHECK(!ThreadState::booted());
    CHECK(ThreadState::tsdnFetch() == nullptr);
    thread_test::takeLog();
    ThreadState * tsd = ThreadState::mallocTsdBoot0();
    REQUIRE(tsd != nullptr);
    CHECK(ThreadState::booted());
    CHECK_EQ(tsd->stateGet(), tsd_state_nominal);
    CHECK(tsd->tcache_enabled);
    CHECK_EQ(tsd, &ThreadState::fetch());
    ThreadState::mallocTsdBoot1();
    CHECK_EQ(tsd->stateGet(), tsd_state_nominal);
    auto log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheTsdDataInit");
    /// `tcacheTsdDataInit` is called on a nominal_slow TSD (`tcache_enabled` is still false).
    CHECK_EQ(log[0].state, tsd_state_nominal_slow);
}

template <typename F>
void runInThread(F && f)
{
    std::thread thread(std::forward<F>(f));
    thread.join();
}

}

TEST(ThreadState, Layout)
{
    static_assert(std::is_trivially_destructible_v<ThreadState>);
    /// The fast fields follow the state; the tcache bins follow the fast counters (jemalloc's aarch64 layout:
    /// `thread_allocated` at state + 8, `tcache.bins[0]` at state + 48).
    CHECK_EQ(offsetof(ThreadState, thread_allocated) - offsetof(ThreadState, state), 8u);
    CHECK_EQ(offsetof(ThreadState, thread_allocated_next_event_fast) - offsetof(ThreadState, thread_allocated), 8u);
    CHECK_EQ(offsetof(ThreadState, thread_deallocated) - offsetof(ThreadState, thread_allocated), 16u);
    CHECK_EQ(offsetof(ThreadState, thread_deallocated_next_event_fast) - offsetof(ThreadState, thread_allocated), 24u);
    CHECK_EQ(offsetof(ThreadState, tcache) + offsetof(ThreadCache, bins) - offsetof(ThreadState, state), 48u);
    CHECK_EQ(offsetof(ThreadState, state) - offsetof(ThreadState, rtree_ctx), sizeof(RadixTreeContext));

    /// TSD_INITIALIZER.
    static constinit ThreadState initial;
    CHECK_EQ(initial.stateGet(), tsd_state_uninitialized);
    CHECK_EQ(initial.binshards.binshard[0], 255);
    CHECK_EQ(initial.binshards.binshard[1], 0);
    CHECK_EQ(initial.arena_decay_ticker.tick, ARENA_DECAY_NTICKS_PER_UPDATE);
    CHECK(initial.tcache.bins[0].stillZeroInitialized());
    CHECK(initial.tcache.tcache_slow == nullptr);
    CHECK_EQ(initial.rtree_ctx.cache[0].leafkey, RTREE_LEAFKEY_INVALID);
}

TEST(ThreadState, BootAndFetch)
{
    boot();
    ThreadState & tsd = ThreadState::fetch();
    CHECK(tsd.fast());
    CHECK_EQ(&tsd, tsd_detail::tlsAddrTsdTls());
    CHECK_EQ(ThreadState::tsdnFetch(), &tsd);
    CHECK_EQ(tsd.prng_state, static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&tsd)));
    CHECK_EQ(tsd.thread_allocated_next_event_fast, tsd.thread_allocated_next_event);
    CHECK_EQ(tsd.thread_allocated_next_event, opt.tcache_gc_incr_bytes);
    CHECK_EQ(tsd.san_extents_until_guard_small, opt.san_guard_small);
    CHECK_EQ(tsd.san_extents_until_guard_large, opt.san_guard_large);
}

TEST(ThreadState, NewThreadFullInit)
{
    boot();
    ThreadState * main_tsd = &ThreadState::fetch();
    thread_test::takeLog();
    ThreadState * thread_tsd = nullptr;
    std::vector<thread_test::HookCall> init_log;
    runInThread([&]
    {
        ThreadState * raw = Tsd::get(false);
        CHECK_EQ(raw->stateGet(), tsd_state_uninitialized);
        thread_tsd = &ThreadState::fetch();
        CHECK_EQ(raw, thread_tsd);
        CHECK_EQ(thread_tsd->stateGet(), tsd_state_nominal);
        CHECK_EQ(thread_tsd->reentrancy_level, 0);
        init_log = thread_test::takeLog();
    });
    CHECK_NE(thread_tsd, main_tsd);
    CHECK((names(init_log) == std::vector<std::string>{"tcacheTsdDataInit"}));

    /// Thread exit: the destructor cleans up in jemalloc's order, then the TSD stays in purgatory (one more destructor
    /// round, which does nothing).
    auto exit_log = thread_test::takeLog();
    CHECK((names(exit_log) == std::vector<std::string>{"profTdataCleanup", "iarenaCleanup", "arenaCleanup", "tcacheCleanup"}));
    for (const auto & call : exit_log)
    {
        CHECK_EQ(call.tsd, thread_tsd);
        CHECK_EQ(call.state, tsd_state_nominal);
        CHECK_EQ(call.reentrancy_level, 0);
    }
}

/// A thread that only frees: the minimal TSD (no cleanup) becomes nominal after 128 fetches.
TEST(ThreadState, MinimalInitialized)
{
    boot();
    thread_test::takeLog();
    runInThread([]
    {
        ThreadState & tsd = ThreadState::fetchMin();
        CHECK_EQ(tsd.stateGet(), tsd_state_minimal_initialized);
        CHECK_EQ(tsd.min_init_state_nfetched, 1);
        CHECK_EQ(tsd.reentrancy_level, 1);
        CHECK(!tsd.tcache_enabled);
        CHECK(!tsd.nominal());
        CHECK(tsd.stateNocleanup());
        CHECK_EQ(tsd.thread_allocated_next_event_fast, 0u);
        CHECK_EQ(tsd.thread_deallocated_next_event_fast, 0u);
        for (int i = 2; i < TSD_MIN_INIT_STATE_MAX_FETCHED; ++i)
        {
            ThreadState::fetchMin();
            CHECK_EQ(tsd.min_init_state_nfetched, i);
            CHECK_EQ(tsd.stateGet(), tsd_state_minimal_initialized);
        }
        CHECK(thread_test::takeLog().empty());
        ThreadState::fetchMin();
        CHECK_EQ(tsd.min_init_state_nfetched, TSD_MIN_INIT_STATE_MAX_FETCHED);
        CHECK_EQ(tsd.stateGet(), tsd_state_nominal);
        CHECK_EQ(tsd.reentrancy_level, 0);
        CHECK(tsd.tcache_enabled);
        CHECK((names(thread_test::takeLog()) == std::vector<std::string>{"tcacheTsdDataInit"}));
    });
    CHECK_EQ(thread_test::takeLog().size(), 4u);

    /// A full fetch on a minimal TSD switches to nominal immediately.
    runInThread([]
    {
        ThreadState & tsd = ThreadState::fetchMin();
        CHECK_EQ(tsd.stateGet(), tsd_state_minimal_initialized);
        ThreadState::fetch();
        CHECK_EQ(tsd.stateGet(), tsd_state_nominal);
        CHECK_EQ(tsd.min_init_state_nfetched, 2);
        CHECK_EQ(tsd.reentrancy_level, 0);
    });
    CHECK_EQ(thread_test::takeLog().size(), 5u);

    /// A minimal TSD that exits is still cleaned up (jemalloc calls the cleanup "for testing and completeness").
    runInThread([] { ThreadState::fetchMin(); });
    auto log = thread_test::takeLog();
    CHECK((names(log) == std::vector<std::string>{"profTdataCleanup", "iarenaCleanup", "arenaCleanup", "tcacheCleanup"}));
    for (const auto & call : log)
    {
        CHECK_EQ(call.state, tsd_state_minimal_initialized);
        CHECK_EQ(call.reentrancy_level, 1);
    }
}

TEST(ThreadState, InternalFetch)
{
    boot();
    thread_test::takeLog();
    runInThread([]
    {
        ThreadState & tsd = ThreadState::internalFetch();
        CHECK_EQ(tsd.stateGet(), tsd_state_reincarnated);
        CHECK_EQ(tsd.reentrancy_level, 1);
        CHECK(!tsd.tcache_enabled);
        /// Reincarnated TSDs stay as they are.
        CHECK_EQ(&ThreadState::fetch(), &tsd);
        CHECK_EQ(tsd.stateGet(), tsd_state_reincarnated);
    });
    auto log = thread_test::takeLog();
    CHECK_EQ(log.size(), 4u);
    for (const auto & call : log)
        CHECK_EQ(call.state, tsd_state_reincarnated);
}

namespace
{

pthread_key_t late_key;
std::atomic<int> late_destructor_calls{0};
std::atomic<uint8_t> late_state_before{0};
std::atomic<uint8_t> late_state_after{0};

/// Runs after the TSD destructor (glibc calls the destructors in key order within a round) and uses the allocator.
void lateDestructor(void *)
{
    ++late_destructor_calls;
    ThreadState * tsd = tsd_detail::tlsAddrTsdTls();
    late_state_before = tsd->stateGet();
    ThreadState & fetched = ThreadState::fetch();
    late_state_after = fetched.stateGet();
}

}

/// A destructor of another library that allocates after the TSD destructor reincarnates the TSD; it is cleaned up
/// again in the next destructor round.
TEST(ThreadState, Reincarnation)
{
    boot();
    REQUIRE(pthread_key_create(&late_key, &lateDestructor) == 0);
    thread_test::takeLog();
    runInThread([]
    {
        ThreadState::fetch();
        pthread_setspecific(late_key, reinterpret_cast<void *>(1));
    });
    CHECK_EQ(late_destructor_calls.load(), 1);
    CHECK_EQ(late_state_before.load(), tsd_state_purgatory);
    CHECK_EQ(late_state_after.load(), tsd_state_reincarnated);
    auto log = thread_test::takeLog();
    CHECK((names(log)
           == std::vector<std::string>{
               "tcacheTsdDataInit",
               "profTdataCleanup",
               "iarenaCleanup",
               "arenaCleanup",
               "tcacheCleanup",
               "profTdataCleanup",
               "iarenaCleanup",
               "arenaCleanup",
               "tcacheCleanup"}));
    if (log.size() == 9)
    {
        CHECK_EQ(log[1].state, tsd_state_nominal);
        CHECK_EQ(log[5].state, tsd_state_reincarnated);
        CHECK_EQ(log[5].reentrancy_level, 1);
    }
    pthread_key_delete(late_key);
}

TEST(ThreadState, Reentrancy)
{
    boot();
    ThreadState & tsd = ThreadState::fetch();
    REQUIRE(tsd.fast());
    uint64_t threshold = tsd.thread_allocated_next_event_fast;
    CHECK_NE(threshold, 0u);

    preReentrancy(tsd, nullptr);
    CHECK_EQ(tsd.reentrancy_level, 1);
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal_slow);
    CHECK_EQ(tsd.thread_allocated_next_event_fast, 0u);
    CHECK_EQ(tsd.thread_deallocated_next_event_fast, 0u);
    /// A fetch on the slow path does nothing.
    CHECK_EQ(&ThreadState::fetch(), &tsd);
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal_slow);

    preReentrancy(tsd, nullptr);
    CHECK_EQ(tsd.reentrancy_level, 2);
    postReentrancy(tsd);
    CHECK_EQ(tsd.reentrancy_level, 1);
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal_slow);
    postReentrancy(tsd);
    CHECK_EQ(tsd.reentrancy_level, 0);
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal);
    CHECK_EQ(tsd.thread_allocated_next_event_fast, threshold);

    /// `malloc_slow` keeps every TSD on the slow path.
    malloc_slow = true;
    tsd.slowUpdate();
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal_slow);
    CHECK_EQ(tsd.thread_allocated_next_event_fast, 0u);
    malloc_slow = false;
    tsd.slowUpdate();
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal);

    /// So does a disabled tcache.
    tsd.tcache_enabled = false;
    tsd.slowUpdate();
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal_slow);
    tsd.tcache_enabled = true;
    tsd.slowUpdate();
    CHECK_EQ(tsd.stateGet(), tsd_state_nominal);
}

/// `globalSlowInc` moves every nominal thread to `nominal_recompute` and zeroes its fast thresholds; each thread
/// recomputes its state at its next fetch.
TEST(ThreadState, GlobalSlow)
{
    boot();
    constexpr int nthreads = 4;
    std::mutex mutex;
    std::condition_variable cv;
    int phase = 0;
    int ready = 0;
    std::vector<ThreadState *> tsds(nthreads);
    std::vector<uint8_t> states_after_inc(nthreads);
    std::vector<uint8_t> states_after_fetch(nthreads);
    std::vector<uint8_t> states_after_dec(nthreads);
    std::vector<uint64_t> fast_after_inc(nthreads);

    auto wait_phase = [&](int p)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return phase >= p; });
    };
    auto report = [&]
    {
        std::lock_guard lock(mutex);
        ++ready;
        cv.notify_all();
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; ++i)
    {
        threads.emplace_back([&, i]
        {
            tsds[i] = &ThreadState::fetch();
            report();
            wait_phase(1);
            states_after_inc[i] = tsds[i]->stateGet();
            fast_after_inc[i] = tsds[i]->thread_allocated_next_event_fast + tsds[i]->thread_deallocated_next_event_fast;
            ThreadState::fetch();
            states_after_fetch[i] = tsds[i]->stateGet();
            report();
            wait_phase(2);
            states_after_dec[i] = tsds[i]->stateGet();
            ThreadState::fetch();
            states_after_fetch[i] = tsds[i]->stateGet();
            report();
        });
    }

    auto wait_ready = [&](int n)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return ready >= n; });
    };

    wait_ready(nthreads);
    CHECK(!ThreadState::globalSlow());
    ThreadState::globalSlowInc(&ThreadState::fetch());
    CHECK(ThreadState::globalSlow());
    ThreadState & main_tsd = ThreadState::fetch(); /// Recomputes the main thread too.
    CHECK_EQ(main_tsd.stateGet(), tsd_state_nominal_slow);
    {
        std::lock_guard lock(mutex);
        phase = 1;
        cv.notify_all();
    }
    wait_ready(2 * nthreads);
    for (int i = 0; i < nthreads; ++i)
    {
        CHECK_EQ(states_after_inc[i], tsd_state_nominal_recompute);
        CHECK_EQ(fast_after_inc[i], 0u);
        CHECK_EQ(states_after_fetch[i], tsd_state_nominal_slow);
    }
    ThreadState::globalSlowDec(&main_tsd);
    CHECK(!ThreadState::globalSlow());
    {
        std::lock_guard lock(mutex);
        phase = 2;
        cv.notify_all();
    }
    wait_ready(3 * nthreads);
    for (int i = 0; i < nthreads; ++i)
    {
        CHECK_EQ(states_after_dec[i], tsd_state_nominal_recompute);
        CHECK_EQ(states_after_fetch[i], tsd_state_nominal);
    }
    CHECK_EQ(main_tsd.stateGet(), tsd_state_nominal_recompute);
    ThreadState::fetch();
    CHECK_EQ(main_tsd.stateGet(), tsd_state_nominal);
    CHECK_NE(main_tsd.thread_allocated_next_event_fast, 0u);
    for (auto & thread : threads)
        thread.join();
    thread_test::takeLog();
}

/// After fork, the child's nominal list contains only the forking thread.
TEST(ThreadState, Fork)
{
    boot();
    ThreadState & tsd = ThreadState::fetch();

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    bool started = false;
    std::thread other([&]
    {
        ThreadState::fetch();
        std::unique_lock lock(mutex);
        started = true;
        cv.notify_all();
        cv.wait(lock, [&] { return done; });
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return started; });
    }

    tsd.prefork();
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0)
    {
        tsd.postforkChild();
        bool ok = tsd.tsd_link.next == &tsd && tsd.tsd_link.prev == &tsd;
        /// The list (and its lock) works in the child.
        ThreadState::globalSlowInc(&tsd);
        ok = ok && tsd.stateGet() == tsd_state_nominal_recompute;
        ThreadState::globalSlowDec(&tsd);
        ThreadState::fetch();
        ok = ok && tsd.stateGet() == tsd_state_nominal;
        _exit(ok ? 0 : 1);
    }
    tsd.postforkParent();
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 0);
    /// The parent still has both threads in the list.
    CHECK(tsd.tsd_link.next != &tsd);

    {
        std::lock_guard lock(mutex);
        done = true;
        cv.notify_all();
    }
    other.join();
    thread_test::takeLog();
}

TEST(ThreadState, MallocThreadCleanup)
{
    /// The FreeBSD cleanup driver: repeats the cleanups that ask for another round.
    static int calls_a = 0;
    static int calls_b = 0;
    mallocTsdCleanupRegister([] { return ++calls_a < 3; });
    mallocTsdCleanupRegister([] { return ++calls_b < 1; });
    mallocThreadCleanup();
    CHECK_EQ(calls_a, 3);
    CHECK_EQ(calls_b, 1);
}

/// The Darwin implementation also works with Linux pthreads: wrappers are allocated with `a0malloc` and freed at
/// thread exit.
TEST(ThreadState, GenericWrapper)
{
    REQUIRE(!TsdGeneric::boot0());
    CHECK(TsdGeneric::is_booted);
    runInThread([]
    {
        CHECK(TsdGeneric::get(false) == nullptr);
        ThreadState * tsd = TsdGeneric::get(true);
        REQUIRE(tsd != nullptr);
        CHECK_EQ(TsdGeneric::get(false), tsd);
        CHECK_EQ(tsd->stateGet(), tsd_state_uninitialized);
        CHECK_EQ(reinterpret_cast<uintptr_t>(TsdGeneric::wrapperGet(false)) % CACHELINE, 0u);
        CHECK(!TsdGeneric::wrapperGet(false)->initialized);
        TsdGeneric::set(tsd);
        CHECK(TsdGeneric::wrapperGet(false)->initialized);
    });
    /// The uninitialized TSD needs no cleanup hooks.
    CHECK(thread_test::takeLog().empty());
}

/// --- Fiber migration ----------------------------------------------------------------------------------------------

namespace
{

constexpr int num_fibers = 32;
constexpr int num_workers = 4;
constexpr int ops_per_fiber = 2000;
constexpr size_t fiber_stack_size = 1 << 16;

ucontext_t fiber_context[num_fibers];
ucontext_t * return_context[num_fibers];
/// The TSD of the worker that resumed the fiber (written by the worker before switching to the fiber).
ThreadState * expected_tsd[num_fibers];
int fiber_remaining_ops[num_fibers];
bool fiber_done[num_fibers];
std::atomic<int> fiber_errors{0};
std::atomic<int> fiber_migrations{0};

std::mutex queue_mutex;
std::vector<int> ready_queue;
int live_fibers = 0;

void pushReadyFiber(int id)
{
    std::lock_guard lock(queue_mutex);
    ready_queue.push_back(id);
}

/// A ready fiber id, -1 if none is ready now, or -2 if all have finished.
int popReadyFiber()
{
    std::lock_guard lock(queue_mutex);
    if (live_fibers == 0)
        return -2;
    if (ready_queue.empty())
        return -1;
    int id = ready_queue.front();
    ready_queue.erase(ready_queue.begin());
    return id;
}

void fiberRun(int id)
{
    ThreadState * previous = nullptr;
    while (fiber_remaining_ops[id] > 0)
    {
        --fiber_remaining_ops[id];
        /// Fetch, count a "deallocation" on the current thread's TSD, yield (possibly to another thread), then fetch
        /// again: the address must be that of the new thread's TSD.
        ThreadState & before = ThreadState::fetch();
        if (&before != expected_tsd[id])
            ++fiber_errors;
        ++before.thread_deallocated;
        swapcontext(&fiber_context[id], return_context[id]);
        ThreadState & after = ThreadState::fetch();
        if (&after != expected_tsd[id])
            ++fiber_errors;
        if (previous != nullptr && previous != &after)
            ++fiber_migrations;
        previous = &after;
        ++after.thread_allocated;
    }
    {
        std::lock_guard lock(queue_mutex);
        fiber_done[id] = true;
        --live_fibers;
    }
    swapcontext(&fiber_context[id], return_context[id]);
}

void workerThread()
{
    ucontext_t scheduler_context;
    ThreadState * own = &ThreadState::fetch();
    for (;;)
    {
        int id = popReadyFiber();
        if (id == -2)
            break;
        if (id < 0)
        {
            std::this_thread::yield();
            continue;
        }
        return_context[id] = &scheduler_context;
        expected_tsd[id] = own;
        swapcontext(&scheduler_context, &fiber_context[id]);
        bool done;
        {
            std::lock_guard lock(queue_mutex);
            done = fiber_done[id];
        }
        if (!done)
            pushReadyFiber(id);
    }
}

}

TEST(ThreadState, FiberMigration)
{
    boot();
    std::vector<std::vector<char>> stacks(num_fibers, std::vector<char>(fiber_stack_size));
    live_fibers = num_fibers;
    for (int i = 0; i < num_fibers; ++i)
    {
        fiber_remaining_ops[i] = ops_per_fiber;
        getcontext(&fiber_context[i]);
        fiber_context[i].uc_stack.ss_sp = stacks[i].data();
        fiber_context[i].uc_stack.ss_size = fiber_stack_size;
        fiber_context[i].uc_link = nullptr;
        makecontext(&fiber_context[i], reinterpret_cast<void (*)()>(&fiberRun), 1, i);
        pushReadyFiber(i);
    }

    std::vector<std::thread> workers;
    for (int i = 0; i < num_workers; ++i)
        workers.emplace_back(&workerThread);
    for (auto & worker : workers)
        worker.join();

    CHECK_EQ(live_fibers, 0);
    CHECK_EQ(fiber_errors.load(), 0);
    /// The test is only meaningful if fibers actually moved between threads.
    CHECK_GT(fiber_migrations.load(), 0);
    thread_test::takeLog();
}

/// The TLS address accessor gives each thread its own TSD, consistent with the offset captured once.
TEST(ThreadState, TlsAddress)
{
    boot();
    ThreadState * main_tsd = tsd_detail::tlsAddrTsdTls();
    CHECK_EQ(main_tsd, &ThreadState::fetch());
    ThreadState * other = nullptr;
    bool * other_initialized = nullptr;
    runInThread([&]
    {
        other = tsd_detail::tlsAddrTsdTls();
        /// `tsd_initialized` exists only with `TsdMallocThreadCleanup` (FreeBSD), like in jemalloc.
        if constexpr (config::tsd_impl == TsdImpl::MallocThreadCleanup)
        {
            other_initialized = tsd_detail::tlsAddrTsdInitialized();
            CHECK(!*other_initialized);
        }
    });
    CHECK_NE(other, main_tsd);
    if constexpr (config::tsd_impl == TsdImpl::MallocThreadCleanup)
        CHECK(other_initialized != tsd_detail::tlsAddrTsdInitialized());
#if ALLOCATOR_TLS_ADDR_FAST
    CHECK_NE(tsd_detail::tls_offset_tsd_tls.load(), tsd_detail::TLS_OFFSET_UNINITIALIZED);
    CHECK_EQ(reinterpret_cast<char *>(main_tsd) - tsd_detail::threadPointer(), tsd_detail::tls_offset_tsd_tls.load());
#endif
}
