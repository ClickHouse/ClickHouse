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
#include <ucontext.h>
#include <unistd.h>
#include <sys/wait.h>

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
    CHECK(ThreadState::threadStateFetch() == nullptr);
    thread_test::takeLog();
    ThreadState * thread_state = ThreadState::mallocThreadStateBoot0();
    REQUIRE(thread_state != nullptr);
    CHECK(ThreadState::booted());
    CHECK_EQ(thread_state->stateGet(), thread_state_nominal);
    CHECK(thread_state->thread_cache_enabled);
    CHECK_EQ(thread_state, &ThreadState::fetch());
    ThreadState::mallocThreadStateBoot1();
    CHECK_EQ(thread_state->stateGet(), thread_state_nominal);
    auto log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheTsdDataInit");
    /// `threadCacheThreadStateDataInit` is called on a nominal_slow TSD (`thread_cache_enabled` is still false).
    CHECK_EQ(log[0].state, thread_state_nominal_slow);
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
    CHECK_EQ(offsetof(ThreadState, thread_cache) + offsetof(ThreadCache, bins) - offsetof(ThreadState, state), 48u);
    CHECK_EQ(offsetof(ThreadState, state) - offsetof(ThreadState, radix_tree_context), sizeof(RadixTreeContext));

    /// TSD_INITIALIZER.
    static constinit ThreadState initial;
    CHECK_EQ(initial.stateGet(), thread_state_uninitialized);
    CHECK_EQ(initial.bin_shards.bin_shard[0], 255);
    CHECK_EQ(initial.bin_shards.bin_shard[1], 0);
    CHECK_EQ(initial.arena_decay_ticker.tick, ARENA_DECAY_NUM_TICKS_PER_UPDATE);
    CHECK(initial.thread_cache.bins[0].stillZeroInitialized());
    CHECK(initial.thread_cache.thread_cache_slow == nullptr);
    CHECK_EQ(initial.radix_tree_context.cache[0].leaf_key, RADIX_TREE_LEAF_KEY_INVALID);
}

TEST(ThreadState, BootAndFetch)
{
    boot();
    ThreadState & thread_state = ThreadState::fetch();
    CHECK(thread_state.fast());
    CHECK_EQ(&thread_state, thread_state_detail::tlsAddrThreadStateTLS());
    CHECK_EQ(ThreadState::threadStateFetch(), &thread_state);
    CHECK_EQ(thread_state.prng_state, static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&thread_state)));
    CHECK_EQ(thread_state.thread_allocated_next_event_fast, thread_state.thread_allocated_next_event);
    CHECK_EQ(thread_state.thread_allocated_next_event, options.thread_cache_gc_increment_bytes);
    CHECK_EQ(thread_state.sanitizer_extents_until_guard_small, options.sanitizer_guard_small);
    CHECK_EQ(thread_state.sanitizer_extents_until_guard_large, options.sanitizer_guard_large);
}

TEST(ThreadState, NewThreadFullInit)
{
    boot();
    ThreadState * main_thread_state = &ThreadState::fetch();
    thread_test::takeLog();
    ThreadState * child_thread_state = nullptr;
    std::vector<thread_test::HookCall> init_log;
    runInThread(
        [&]
        {
            ThreadState * raw = ThreadStateStorage::get(false);
            CHECK_EQ(raw->stateGet(), thread_state_uninitialized);
            child_thread_state = &ThreadState::fetch();
            CHECK_EQ(raw, child_thread_state);
            CHECK_EQ(child_thread_state->stateGet(), thread_state_nominal);
            CHECK_EQ(child_thread_state->reentrancy_level, 0);
            init_log = thread_test::takeLog();
        });
    CHECK_NE(child_thread_state, main_thread_state);
    CHECK((names(init_log) == std::vector<std::string>{"tcacheTsdDataInit"}));

    /// Thread exit: the destructor cleans up in jemalloc's order, then the TSD stays in purgatory (one more destructor
    /// round, which does nothing).
    auto exit_log = thread_test::takeLog();
    CHECK((names(exit_log) == std::vector<std::string>{"profTdataCleanup", "iarenaCleanup", "arenaCleanup", "tcacheCleanup"}));
    for (const auto & call : exit_log)
    {
        CHECK_EQ(call.thread_state, child_thread_state);
        CHECK_EQ(call.state, thread_state_nominal);
        CHECK_EQ(call.reentrancy_level, 0);
    }
}

/// A thread that only frees: the minimal TSD (no cleanup) becomes nominal after 128 fetches.
TEST(ThreadState, MinimalInitialized)
{
    boot();
    thread_test::takeLog();
    runInThread(
        []
        {
            ThreadState & thread_state = ThreadState::fetchMin();
            CHECK_EQ(thread_state.stateGet(), thread_state_minimal_initialized);
            CHECK_EQ(thread_state.min_init_state_num_fetched, 1);
            CHECK_EQ(thread_state.reentrancy_level, 1);
            CHECK(!thread_state.thread_cache_enabled);
            CHECK(!thread_state.nominal());
            CHECK(thread_state.stateNoCleanup());
            CHECK_EQ(thread_state.thread_allocated_next_event_fast, 0u);
            CHECK_EQ(thread_state.thread_deallocated_next_event_fast, 0u);
            for (int i = 2; i < THREAD_STATE_MIN_INIT_STATE_MAX_FETCHED; ++i)
            {
                ThreadState::fetchMin();
                CHECK_EQ(thread_state.min_init_state_num_fetched, i);
                CHECK_EQ(thread_state.stateGet(), thread_state_minimal_initialized);
            }
            CHECK(thread_test::takeLog().empty());
            ThreadState::fetchMin();
            CHECK_EQ(thread_state.min_init_state_num_fetched, THREAD_STATE_MIN_INIT_STATE_MAX_FETCHED);
            CHECK_EQ(thread_state.stateGet(), thread_state_nominal);
            CHECK_EQ(thread_state.reentrancy_level, 0);
            CHECK(thread_state.thread_cache_enabled);
            CHECK((names(thread_test::takeLog()) == std::vector<std::string>{"tcacheTsdDataInit"}));
        });
    CHECK_EQ(thread_test::takeLog().size(), 4u);

    /// A full fetch on a minimal TSD switches to nominal immediately.
    runInThread(
        []
        {
            ThreadState & thread_state = ThreadState::fetchMin();
            CHECK_EQ(thread_state.stateGet(), thread_state_minimal_initialized);
            ThreadState::fetch();
            CHECK_EQ(thread_state.stateGet(), thread_state_nominal);
            CHECK_EQ(thread_state.min_init_state_num_fetched, 2);
            CHECK_EQ(thread_state.reentrancy_level, 0);
        });
    CHECK_EQ(thread_test::takeLog().size(), 5u);

    /// A minimal TSD that exits is still cleaned up (jemalloc calls the cleanup "for testing and completeness").
    runInThread([] { ThreadState::fetchMin(); });
    auto log = thread_test::takeLog();
    CHECK((names(log) == std::vector<std::string>{"profTdataCleanup", "iarenaCleanup", "arenaCleanup", "tcacheCleanup"}));
    for (const auto & call : log)
    {
        CHECK_EQ(call.state, thread_state_minimal_initialized);
        CHECK_EQ(call.reentrancy_level, 1);
    }
}

TEST(ThreadState, InternalFetch)
{
    boot();
    thread_test::takeLog();
    runInThread(
        []
        {
            ThreadState & thread_state = ThreadState::internalFetch();
            CHECK_EQ(thread_state.stateGet(), thread_state_reincarnated);
            CHECK_EQ(thread_state.reentrancy_level, 1);
            CHECK(!thread_state.thread_cache_enabled);
            /// Reincarnated TSDs stay as they are.
            CHECK_EQ(&ThreadState::fetch(), &thread_state);
            CHECK_EQ(thread_state.stateGet(), thread_state_reincarnated);
        });
    auto log = thread_test::takeLog();
    CHECK_EQ(log.size(), 4u);
    for (const auto & call : log)
        CHECK_EQ(call.state, thread_state_reincarnated);
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
    ThreadState * thread_state = thread_state_detail::tlsAddrThreadStateTLS();
    late_state_before = thread_state->stateGet();
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
    runInThread(
        []
        {
            ThreadState::fetch();
            pthread_setspecific(late_key, reinterpret_cast<void *>(1));
        });
    CHECK_EQ(late_destructor_calls.load(), 1);
    CHECK_EQ(late_state_before.load(), thread_state_purgatory);
    CHECK_EQ(late_state_after.load(), thread_state_reincarnated);
    auto log = thread_test::takeLog();
    CHECK(
        (names(log)
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
        CHECK_EQ(log[1].state, thread_state_nominal);
        CHECK_EQ(log[5].state, thread_state_reincarnated);
        CHECK_EQ(log[5].reentrancy_level, 1);
    }
    pthread_key_delete(late_key);
}

TEST(ThreadState, Reentrancy)
{
    boot();
    ThreadState & thread_state = ThreadState::fetch();
    REQUIRE(thread_state.fast());
    uint64_t threshold = thread_state.thread_allocated_next_event_fast;
    CHECK_NE(threshold, 0u);

    preReentrancy(thread_state, nullptr);
    CHECK_EQ(thread_state.reentrancy_level, 1);
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal_slow);
    CHECK_EQ(thread_state.thread_allocated_next_event_fast, 0u);
    CHECK_EQ(thread_state.thread_deallocated_next_event_fast, 0u);
    /// A fetch on the slow path does nothing.
    CHECK_EQ(&ThreadState::fetch(), &thread_state);
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal_slow);

    preReentrancy(thread_state, nullptr);
    CHECK_EQ(thread_state.reentrancy_level, 2);
    postReentrancy(thread_state);
    CHECK_EQ(thread_state.reentrancy_level, 1);
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal_slow);
    postReentrancy(thread_state);
    CHECK_EQ(thread_state.reentrancy_level, 0);
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal);
    CHECK_EQ(thread_state.thread_allocated_next_event_fast, threshold);

    /// `malloc_slow` keeps every TSD on the slow path.
    malloc_slow = true;
    thread_state.slowUpdate();
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal_slow);
    CHECK_EQ(thread_state.thread_allocated_next_event_fast, 0u);
    malloc_slow = false;
    thread_state.slowUpdate();
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal);

    /// So does a disabled tcache.
    thread_state.thread_cache_enabled = false;
    thread_state.slowUpdate();
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal_slow);
    thread_state.thread_cache_enabled = true;
    thread_state.slowUpdate();
    CHECK_EQ(thread_state.stateGet(), thread_state_nominal);
}

/// `globalSlowIncrement` moves every nominal thread to `nominal_recompute` and zeroes its fast thresholds; each thread
/// recomputes its state at its next fetch.
TEST(ThreadState, GlobalSlow)
{
    boot();
    constexpr int num_threads = 4;
    std::mutex mutex;
    std::condition_variable condition;
    int phase = 0;
    int ready = 0;
    std::vector<ThreadState *> thread_states(num_threads);
    std::vector<uint8_t> states_after_increment(num_threads);
    std::vector<uint8_t> states_after_fetch(num_threads);
    std::vector<uint8_t> states_after_decrement(num_threads);
    std::vector<uint64_t> fast_after_increment(num_threads);

    auto wait_phase = [&](int p)
    {
        std::unique_lock lock(mutex);
        condition.wait(lock, [&] { return phase >= p; });
    };
    auto report = [&]
    {
        std::lock_guard lock(mutex);
        ++ready;
        condition.notify_all();
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(
            [&, i]
            {
                thread_states[i] = &ThreadState::fetch();
                report();
                wait_phase(1);
                states_after_increment[i] = thread_states[i]->stateGet();
                fast_after_increment[i]
                    = thread_states[i]->thread_allocated_next_event_fast + thread_states[i]->thread_deallocated_next_event_fast;
                ThreadState::fetch();
                states_after_fetch[i] = thread_states[i]->stateGet();
                report();
                wait_phase(2);
                states_after_decrement[i] = thread_states[i]->stateGet();
                ThreadState::fetch();
                states_after_fetch[i] = thread_states[i]->stateGet();
                report();
            });
    }

    auto wait_ready = [&](int n)
    {
        std::unique_lock lock(mutex);
        condition.wait(lock, [&] { return ready >= n; });
    };

    wait_ready(num_threads);
    CHECK(!ThreadState::globalSlow());
    ThreadState::globalSlowIncrement(&ThreadState::fetch());
    CHECK(ThreadState::globalSlow());
    ThreadState & main_thread_state = ThreadState::fetch(); /// Recomputes the main thread too.
    CHECK_EQ(main_thread_state.stateGet(), thread_state_nominal_slow);
    {
        std::lock_guard lock(mutex);
        phase = 1;
        condition.notify_all();
    }
    wait_ready(2 * num_threads);
    for (int i = 0; i < num_threads; ++i)
    {
        CHECK_EQ(states_after_increment[i], thread_state_nominal_recompute);
        CHECK_EQ(fast_after_increment[i], 0u);
        CHECK_EQ(states_after_fetch[i], thread_state_nominal_slow);
    }
    ThreadState::globalSlowDecrement(&main_thread_state);
    CHECK(!ThreadState::globalSlow());
    {
        std::lock_guard lock(mutex);
        phase = 2;
        condition.notify_all();
    }
    wait_ready(3 * num_threads);
    for (int i = 0; i < num_threads; ++i)
    {
        CHECK_EQ(states_after_decrement[i], thread_state_nominal_recompute);
        CHECK_EQ(states_after_fetch[i], thread_state_nominal);
    }
    CHECK_EQ(main_thread_state.stateGet(), thread_state_nominal_recompute);
    ThreadState::fetch();
    CHECK_EQ(main_thread_state.stateGet(), thread_state_nominal);
    CHECK_NE(main_thread_state.thread_allocated_next_event_fast, 0u);
    for (auto & thread : threads)
        thread.join();
    thread_test::takeLog();
}

/// After fork, the child's nominal list contains only the forking thread.
TEST(ThreadState, Fork)
{
    boot();
    ThreadState & thread_state = ThreadState::fetch();

    std::mutex mutex;
    std::condition_variable condition;
    bool done = false;
    bool started = false;
    std::thread other(
        [&]
        {
            ThreadState::fetch();
            std::unique_lock lock(mutex);
            started = true;
            condition.notify_all();
            condition.wait(lock, [&] { return done; });
        });
    {
        std::unique_lock lock(mutex);
        condition.wait(lock, [&] { return started; });
    }

    thread_state.prefork();
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0)
    {
        thread_state.postforkChild();
        bool ok = thread_state.thread_state_link.next == &thread_state && thread_state.thread_state_link.prev == &thread_state;
        /// The list (and its lock) works in the child.
        ThreadState::globalSlowIncrement(&thread_state);
        ok = ok && thread_state.stateGet() == thread_state_nominal_recompute;
        ThreadState::globalSlowDecrement(&thread_state);
        ThreadState::fetch();
        ok = ok && thread_state.stateGet() == thread_state_nominal;
        _exit(ok ? 0 : 1);
    }
    thread_state.postforkParent();
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 0);
    /// The parent still has both threads in the list.
    CHECK(thread_state.thread_state_link.next != &thread_state);

    {
        std::lock_guard lock(mutex);
        done = true;
        condition.notify_all();
    }
    other.join();
    thread_test::takeLog();
}

TEST(ThreadState, MallocThreadCleanup)
{
    /// The FreeBSD cleanup driver: repeats the cleanups that ask for another round.
    static int calls_a = 0;
    static int calls_b = 0;
    mallocThreadStateCleanupRegister([] { return ++calls_a < 3; });
    mallocThreadStateCleanupRegister([] { return ++calls_b < 1; });
    mallocThreadCleanup();
    CHECK_EQ(calls_a, 3);
    CHECK_EQ(calls_b, 1);
}

/// The Darwin implementation also works with Linux pthreads: wrappers are allocated with `arena0Allocate` and freed at
/// thread exit.
TEST(ThreadState, GenericWrapper)
{
    REQUIRE(!ThreadStateGeneric::boot0());
    CHECK(ThreadStateGeneric::is_booted);
    runInThread(
        []
        {
            CHECK(ThreadStateGeneric::get(false) == nullptr);
            ThreadState * thread_state = ThreadStateGeneric::get(true);
            REQUIRE(thread_state != nullptr);
            CHECK_EQ(ThreadStateGeneric::get(false), thread_state);
            CHECK_EQ(thread_state->stateGet(), thread_state_uninitialized);
            CHECK_EQ(reinterpret_cast<uintptr_t>(ThreadStateGeneric::wrapperGet(false)) % CACHE_LINE, 0u);
            CHECK(!ThreadStateGeneric::wrapperGet(false)->initialized);
            ThreadStateGeneric::set(thread_state);
            CHECK(ThreadStateGeneric::wrapperGet(false)->initialized);
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
ThreadState * expected_thread_state[num_fibers];
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
        if (&before != expected_thread_state[id])
            ++fiber_errors;
        ++before.thread_deallocated;
        swapcontext(&fiber_context[id], return_context[id]);
        ThreadState & after = ThreadState::fetch();
        if (&after != expected_thread_state[id])
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
        expected_thread_state[id] = own;
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
TEST(ThreadState, TLSAddress)
{
    boot();
    ThreadState * main_thread_state = thread_state_detail::tlsAddrThreadStateTLS();
    CHECK_EQ(main_thread_state, &ThreadState::fetch());
    ThreadState * other = nullptr;
    bool * other_initialized = nullptr;
    runInThread(
        [&]
        {
            other = thread_state_detail::tlsAddrThreadStateTLS();
            /// `thread_state_initialized` exists only with `ThreadStateMallocThreadCleanup` (FreeBSD), like in jemalloc.
            if constexpr (config::thread_state_impl == ThreadStateImpl::MallocThreadCleanup)
            {
                other_initialized = thread_state_detail::tlsAddrThreadStateInitialized();
                CHECK(!*other_initialized);
            }
        });
    CHECK_NE(other, main_thread_state);
    if constexpr (config::thread_state_impl == ThreadStateImpl::MallocThreadCleanup)
        CHECK(other_initialized != thread_state_detail::tlsAddrThreadStateInitialized());
#if ALLOCATOR_TLS_ADDR_FAST
    CHECK_NE(thread_state_detail::tls_offset_thread_state_tls.load(), thread_state_detail::TLS_OFFSET_UNINITIALIZED);
    CHECK_EQ(
        reinterpret_cast<char *>(main_thread_state) - thread_state_detail::threadPointer(),
        thread_state_detail::tls_offset_thread_state_tls.load());
#endif
}
