/// Thread events: the exact wait / threshold sequences for allocation and deallocation streams, compared with an
/// independent model of jemalloc's formulas (`thread_event.c`: `te_init`, `te_event_trigger`, `te_update_wait`), the
/// handler order, postponement, the fast thresholds, the peak tracker and `counter_accum`.

#include <allocator/Options.h>
#include <allocator/ThreadEvent.h>

#include "Test.h"
#include "ThreadTestHooks.h"

#include <memory>
#include <string>
#include <vector>

using namespace jemalloc;

namespace
{

/// A TSD that is not in TLS, in the nominal state (as after `tsd_fetch`).
std::unique_ptr<ThreadState> makeNominalThreadState(uint64_t prng_seed, uint64_t allocated = 0, uint64_t deallocated = 0)
{
    auto thread_state = std::make_unique<ThreadState>();
    thread_state->state.store(thread_state_nominal, std::memory_order_relaxed);
    thread_state->thread_cache_enabled = true;
    thread_state->prng_state = prng_seed;
    thread_state->thread_allocated = allocated;
    thread_state->thread_deallocated = deallocated;
    threadStateThreadEventInit(*thread_state);
    return thread_state;
}

/// An independent model of jemalloc's event computation.
struct Model
{
    /// Waits indexed as `te_alloc_*` / `te_dalloc_*`.
    uint64_t alloc_wait[thread_event_allocation_count] = {};
    uint64_t deallocation_wait[thread_event_deallocation_count] = {};
    uint64_t current[2] = {};
    uint64_t last[2] = {};
    uint64_t next[2] = {};
    uint64_t prng = 0;
    bool nominal = true;
    int reentrancy = 0;
    std::vector<std::string> triggered;

    bool profilingOn() const { return options.profiling; }
    bool statsOn() const { return options.stats_interval >= 0; }

    uint64_t newWait(int handler)
    {
        switch (handler)
        {
            case 0: return thread_test::profilingGeometricWait(prng, thread_test::log2_profiling_sample);
            case 1: return stats_interval_accumulated_batch;
            case 2: return options.thread_cache_gc_increment_bytes;
            default: return PEAK_EVENT_WAIT;
        }
    }

    uint64_t postponedWait(int handler) { return handler == 0 ? newWait(0) : 1; }

    void init(uint64_t allocated, uint64_t deallocated)
    {
        current[0] = allocated;
        current[1] = deallocated;
        for (int a = 0; a < 2; ++a)
        {
            last[a] = current[a];
            uint64_t wait = UINT64_MAX;
            /// The table order: prof_sample, stats_interval, tcache_gc, peak (alloc); tcache_gc, peak (dalloc).
            if (a == 0)
            {
                bool enabled[4] = {profilingOn(), statsOn(), options.thread_cache_gc_increment_bytes > 0, true};
                for (int h = 0; h < 4; ++h)
                {
                    if (!enabled[h])
                        continue;
                    alloc_wait[h] = newWait(h);
                    wait = std::min(wait, alloc_wait[h]);
                }
            }
            else
            {
                deallocation_wait[0] = newWait(2);
                deallocation_wait[1] = newWait(3);
                wait = std::min(deallocation_wait[0], deallocation_wait[1]);
            }
            next[a] = last[a] + std::min(wait, THREAD_EVENT_MAX_INTERVAL);
        }
    }

    void update(uint64_t & event_wait, uint64_t accumulated, bool allow, int handler, uint64_t new_wait, uint64_t & wait, const char * name)
    {
        if (event_wait > accumulated)
            event_wait -= accumulated;
        else if (!allow)
            event_wait = postponedWait(handler);
        else
        {
            triggered.push_back(name);
            event_wait = new_wait == 0 ? newWait(handler) : new_wait;
        }
        wait = std::min(wait, event_wait);
    }

    void event(size_t usable_size, bool is_alloc)
    {
        int a = is_alloc ? 0 : 1;
        uint64_t before = current[a];
        current[a] += usable_size;
        if (usable_size < next[a] - before)
            return;
        uint64_t accumulated = current[a] - last[a];
        last[a] = current[a];
        bool allow = nominal && reentrancy == 0;
        uint64_t wait = UINT64_MAX;
        if (is_alloc)
        {
            update(
                alloc_wait[thread_event_allocation_thread_cache_gc],
                accumulated,
                allow,
                2,
                options.thread_cache_gc_increment_bytes,
                wait,
                "tcacheGcEvent");
            if (profilingOn())
                update(alloc_wait[thread_event_allocation_profiling_sample], accumulated, allow, 0, 0, wait, "profSampleEvent");
            if (statsOn())
                update(
                    alloc_wait[thread_event_allocation_stats_interval],
                    accumulated,
                    allow,
                    1,
                    stats_interval_accumulated_batch,
                    wait,
                    "statsIntervalEvent");
            update(alloc_wait[thread_event_allocation_peak], accumulated, allow, 3, PEAK_EVENT_WAIT, wait, "peak");
        }
        else
        {
            update(
                deallocation_wait[thread_event_deallocation_thread_cache_gc],
                accumulated,
                allow,
                2,
                options.thread_cache_gc_increment_bytes,
                wait,
                "tcacheGcEvent");
            update(deallocation_wait[thread_event_deallocation_peak], accumulated, allow, 3, PEAK_EVENT_WAIT, wait, "peak");
        }
        next[a] = last[a] + std::min(wait, THREAD_EVENT_MAX_INTERVAL);
    }
};

void compare(ThreadState & thread_state, const Model & model, size_t step)
{
    int failures = allocator_test::failureCount();
    CHECK_EQ(thread_state.thread_allocated, model.current[0]);
    CHECK_EQ(thread_state.thread_allocated_last_event, model.last[0]);
    CHECK_EQ(thread_state.thread_allocated_next_event, model.next[0]);
    CHECK_EQ(thread_state.thread_deallocated, model.current[1]);
    CHECK_EQ(thread_state.thread_deallocated_last_event, model.last[1]);
    CHECK_EQ(thread_state.thread_deallocated_next_event, model.next[1]);
    for (unsigned i = 0; i < thread_event_allocation_count; ++i)
        CHECK_EQ(thread_state.thread_event_data.alloc_wait[i], model.alloc_wait[i]);
    for (unsigned i = 0; i < thread_event_deallocation_count; ++i)
        CHECK_EQ(thread_state.thread_event_data.deallocation_wait[i], model.deallocation_wait[i]);
    CHECK_EQ(thread_state.prng_state, model.prng);
    bool fast = thread_state.stateGet() == thread_state_nominal;
    CHECK_EQ(thread_state.thread_allocated_next_event_fast, fast && model.next[0] <= THREAD_EVENT_NEXT_EVENT_FAST_MAX ? model.next[0] : 0);
    CHECK_EQ(
        thread_state.thread_deallocated_next_event_fast, fast && model.next[1] <= THREAD_EVENT_NEXT_EVENT_FAST_MAX ? model.next[1] : 0);
    if (allocator_test::failureCount() != failures)
    {
        std::fprintf(stderr, "  at step %zu\n", step);
        allocator_test::abortTest();
    }
}

/// The names of the handlers called since the last call (peak is not a hook: detected via `peak.cur_max` updates is
/// not possible in general, so the model's "peak" entries are filtered out).
std::vector<std::string> takeTriggered()
{
    std::vector<std::string> names;
    for (auto & call : thread_test::takeLog())
        names.push_back(call.name);
    return names;
}

std::vector<std::string> withoutPeak(std::vector<std::string> names)
{
    std::erase(names, std::string("peak"));
    return names;
}

struct Rng
{
    uint64_t x;

    uint64_t next()
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    }
};

/// Random streams of allocations and deallocations of random sizes (mostly small, sometimes huge), with
/// reentrancy / non-nominal periods.
void runStream(uint64_t seed, size_t num_steps, uint64_t start_allocated, uint64_t start_deallocated)
{
    thread_test::takeLog();
    auto thread_state = makeNominalThreadState(seed, start_allocated, start_deallocated);
    Model model;
    model.prng = seed;
    model.init(start_allocated, start_deallocated);
    compare(*thread_state, model, 0);
    CHECK(takeTriggered().empty());

    Rng rng{seed | 1};
    for (size_t step = 1; step <= num_steps; ++step)
    {
        uint64_t r = rng.next();
        if (r % 97 == 0)
        {
            /// Enter or leave reentrancy (non-nominal fast state; events are postponed).
            if (thread_state->reentrancy_level == 0)
            {
                preReentrancy(*thread_state, nullptr);
                CHECK_EQ(thread_state->stateGet(), thread_state_nominal_slow);
                model.reentrancy = 1;
            }
            else
            {
                postReentrancy(*thread_state);
                CHECK_EQ(thread_state->stateGet(), thread_state_nominal);
                model.reentrancy = 0;
            }
            compare(*thread_state, model, step);
            continue;
        }
        size_t usable_size;
        switch ((r >> 8) % 8)
        {
            case 0: usable_size = (r >> 16) % (8 << 20); break;
            case 1: usable_size = (r >> 16) % 300000; break;
            default: usable_size = 8 + (r >> 16) % 4096; break;
        }
        bool is_alloc = (r >> 12) % 3 != 0;
        if (is_alloc)
            threadAllocationEvent(*thread_state, usable_size);
        else
            threadDeallocationEvent(*thread_state, usable_size);
        model.event(usable_size, is_alloc);
        compare(*thread_state, model, step);
        CHECK(withoutPeak(model.triggered) == takeTriggered());
        model.triggered.clear();
    }
}

struct OptionsGuard
{
    Options saved = options;
    unsigned saved_log2_profiling_sample = thread_test::log2_profiling_sample;
    uint64_t saved_batch = stats_interval_accumulated_batch;
    ~OptionsGuard()
    {
        options = saved;
        thread_test::log2_profiling_sample = saved_log2_profiling_sample;
        stats_interval_accumulated_batch = saved_batch;
    }
};

}

TEST(ThreadEvent, Constants)
{
    CHECK_EQ(THREAD_EVENT_MIN_START_WAIT, 1u);
    CHECK_EQ(THREAD_EVENT_MAX_START_WAIT, UINT64_MAX);
    CHECK_EQ(THREAD_EVENT_NEXT_EVENT_FAST_MAX, UINT64_MAX - 4096 + 1);
    CHECK_EQ(THREAD_EVENT_MAX_INTERVAL, 4u << 20);
    CHECK_EQ(PEAK_EVENT_WAIT, 65536u);
    CHECK_EQ(unsigned(thread_event_allocation_count), 8u);
    CHECK_EQ(unsigned(thread_event_deallocation_count), 6u);
}

/// Default options (no prof, no stats interval): the only events are tcache GC and peak, both every 64 KiB.
TEST(ThreadEvent, DefaultSequence)
{
    OptionsGuard guard;
    thread_test::takeLog();
    auto thread_state = makeNominalThreadState(12345);
    CHECK_EQ(thread_state->thread_allocated_last_event, 0u);
    CHECK_EQ(thread_state->thread_allocated_next_event, 65536u);
    CHECK_EQ(thread_state->thread_allocated_next_event_fast, 65536u);
    CHECK_EQ(thread_state->thread_deallocated_next_event, 65536u);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_thread_cache_gc], 65536u);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_peak], 65536u);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_profiling_sample], 0u);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_stats_interval], 0u);
    CHECK_EQ(thread_state->thread_event_data.deallocation_wait[thread_event_deallocation_thread_cache_gc], 65536u);
    CHECK_EQ(thread_state->thread_event_data.deallocation_wait[thread_event_deallocation_peak], 65536u);

    threadAllocationEvent(*thread_state, 65535);
    CHECK(takeTriggered().empty());
    CHECK_EQ(thread_state->thread_allocated, 65535u);
    CHECK_EQ(thread_state->thread_allocated_next_event, 65536u);

    /// No carry-over of the overshoot: the next waits start from the triggering byte count.
    threadAllocationEvent(*thread_state, 100);
    auto log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheGcEvent");
    CHECK_EQ(log[0].allocated, 65635u);
    CHECK_EQ(thread_state->thread_allocated_last_event, 65635u);
    CHECK_EQ(thread_state->thread_allocated_next_event, 65635u + 65536);
    CHECK_EQ(thread_state->peak.current_max, 65635u);

    threadDeallocationEvent(*thread_state, 70000);
    log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheGcEvent");
    CHECK_EQ(thread_state->thread_deallocated_next_event, 70000u + 65536);
    CHECK_EQ(thread_state->peak.current_max, 65635u);

    /// A huge allocation crosses several waits at once; it triggers each event once.
    threadAllocationEvent(*thread_state, 10 << 20);
    log = thread_test::takeLog();
    CHECK_EQ(log.size(), 1u);
    CHECK_EQ(thread_state->thread_allocated_next_event, 65635u + (10 << 20) + 65536);
    CHECK_EQ(thread_state->peak.current_max, 65635u + (10 << 20) - 70000);
}

/// Without any event with a short wait the threshold is capped at `THREAD_EVENT_MAX_INTERVAL`.
TEST(ThreadEvent, MaxInterval)
{
    OptionsGuard guard;
    options.thread_cache_gc_increment_bytes = 100 << 20;
    auto thread_state = makeNominalThreadState(1);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_thread_cache_gc], uint64_t(100) << 20);
    CHECK_EQ(thread_state->thread_allocated_next_event, PEAK_EVENT_WAIT);
    /// Peak triggers every 64 KiB, so the GC wait is decremented by the accumulated bytes.
    threadAllocationEvent(*thread_state, 65536);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_thread_cache_gc], (uint64_t(100) << 20) - 65536);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_peak], PEAK_EVENT_WAIT);
}

/// Events are postponed (wait 1) while the TSD is reentrant, and run at the next event after leaving.
TEST(ThreadEvent, Postponed)
{
    OptionsGuard guard;
    thread_test::takeLog();
    auto thread_state = makeNominalThreadState(7);
    preReentrancy(*thread_state, nullptr);
    CHECK_EQ(thread_state->thread_allocated_next_event_fast, 0u);
    CHECK_EQ(thread_state->thread_deallocated_next_event_fast, 0u);
    threadAllocationEvent(*thread_state, 70000);
    CHECK(takeTriggered().empty());
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_thread_cache_gc], 1u);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_peak], 1u);
    CHECK_EQ(thread_state->thread_allocated_next_event, 70001u);
    CHECK_EQ(thread_state->thread_allocated_next_event_fast, 0u);
    postReentrancy(*thread_state);
    CHECK_EQ(thread_state->thread_allocated_next_event_fast, 70001u);
    threadAllocationEvent(*thread_state, 8);
    auto log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheGcEvent");
    CHECK_EQ(thread_state->thread_allocated_next_event, 70008u + 65536);
}

/// With prof and stats interval on, the alloc handlers run in the order gc, prof, stats_interval, peak.
TEST(ThreadEvent, HandlerOrder)
{
    OptionsGuard guard;
    options.profiling = true;
    options.stats_interval = 0;
    stats_interval_accumulated_batch = 1;
    thread_test::log2_profiling_sample = 0; /// Wait 1: prof triggers on every event.
    thread_test::takeLog();
    auto thread_state = makeNominalThreadState(99);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_profiling_sample], 1u);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_stats_interval], 1u);
    CHECK_EQ(thread_state->thread_allocated_next_event, 1u);
    threadAllocationEvent(*thread_state, 65536);
    auto names = takeTriggered();
    CHECK((names == std::vector<std::string>{"tcacheGcEvent", "profSampleEvent", "statsIntervalEvent"}));
    CHECK_EQ(thread_state->peak.current_max, 65536u);
    threadAllocationEvent(*thread_state, 16);
    names = takeTriggered();
    CHECK((names == std::vector<std::string>{"profSampleEvent", "statsIntervalEvent"}));
}

/// The prof sample waits are drawn from the TSD's PRNG, in table order at init (prof first).
TEST(ThreadEvent, ProfilingWaits)
{
    OptionsGuard guard;
    options.profiling = true;
    thread_test::log2_profiling_sample = 19;
    uint64_t seed = 0x123456789;
    auto thread_state = makeNominalThreadState(seed);
    uint64_t expected_prng = seed;
    uint64_t expected_wait = thread_test::profilingGeometricWait(expected_prng, 19);
    CHECK_EQ(thread_state->thread_event_data.alloc_wait[thread_event_allocation_profiling_sample], expected_wait);
    CHECK_EQ(thread_state->prng_state, expected_prng);
    CHECK_EQ(thread_state->thread_allocated_next_event, std::min(expected_wait, PEAK_EVENT_WAIT));
}

TEST(ThreadEvent, RandomStreamsDefault)
{
    OptionsGuard guard;
    for (uint64_t seed = 1; seed <= 10; ++seed)
        runStream(seed * 0x9e3779b97f4a7c15ULL, 20000, 0, 0);
}

TEST(ThreadEvent, RandomStreamsProfiling)
{
    OptionsGuard guard;
    options.profiling = true;
    for (unsigned log2_size : {19u, 12u, 0u})
    {
        thread_test::log2_profiling_sample = log2_size;
        for (uint64_t seed = 1; seed <= 5; ++seed)
            runStream(seed * 0x2545f4914f6cdd1dULL + log2_size, 20000, 0, 0);
    }
}

TEST(ThreadEvent, RandomStreamsStatsInterval)
{
    OptionsGuard guard;
    options.profiling = true;
    options.stats_interval = 1 << 20;
    stats_interval_accumulated_batch = (1 << 20) >> 6;
    options.thread_cache_gc_increment_bytes = 1024;
    for (uint64_t seed = 1; seed <= 5; ++seed)
        runStream(seed * 0x5851f42d4c957f2dULL, 20000, 0, 0);
}

/// Counters near the 64-bit wrap: the fast threshold is 0 while `next_event > THREAD_EVENT_NEXT_EVENT_FAST_MAX`.
TEST(ThreadEvent, Wraparound)
{
    OptionsGuard guard;
    auto thread_state = makeNominalThreadState(5, UINT64_MAX - 67000, UINT64_MAX - 10);
    CHECK_EQ(thread_state->thread_allocated_next_event, UINT64_MAX - 67000 + 65536);
    CHECK_EQ(thread_state->thread_allocated_next_event_fast, 0u);
    CHECK_EQ(thread_state->thread_deallocated_next_event, uint64_t(65536 - 11));
    CHECK_EQ(thread_state->thread_deallocated_next_event_fast, uint64_t(65536 - 11));
    for (uint64_t seed = 1; seed <= 5; ++seed)
        runStream(seed * 0x9e3779b97f4a7c15ULL, 5000, UINT64_MAX - seed * 50000, UINT64_MAX - seed * 3000);
}

TEST(ThreadEvent, Peak)
{
    OptionsGuard guard;
    auto thread_state = makeNominalThreadState(3);
    thread_state->thread_allocated = 1000;
    thread_state->thread_deallocated = 200;
    peakEventUpdate(*thread_state);
    CHECK_EQ(peakEventMax(*thread_state), 800u);
    thread_state->thread_deallocated = 900;
    peakEventUpdate(*thread_state);
    CHECK_EQ(peakEventMax(*thread_state), 800u);
    peakEventZero(*thread_state);
    CHECK_EQ(peakEventMax(*thread_state), 0u);
    CHECK_EQ(thread_state->peak.adjustment, 100u);
    thread_state->thread_allocated = 1100;
    peakEventUpdate(*thread_state);
    CHECK_EQ(peakEventMax(*thread_state), 100u);
    /// A negative candidate (more deallocated than allocated since the reset) does not lower the peak.
    thread_state->thread_deallocated = 2000;
    peakEventUpdate(*thread_state);
    CHECK_EQ(peakEventMax(*thread_state), 100u);

    struct Activity
    {
        int calls = 0;
        uint64_t allocated = 0;
        uint64_t deallocated = 0;
    } activity;
    thread_state->activity_callback_thunk.callback = [](void * user_context, uint64_t allocated, uint64_t deallocated)
    {
        auto * a = static_cast<Activity *>(user_context);
        ++a->calls;
        a->allocated = allocated;
        a->deallocated = deallocated;
    };
    thread_state->activity_callback_thunk.user_context = &activity;
    peakEvent(*thread_state);
    CHECK_EQ(activity.calls, 1);
    CHECK_EQ(activity.allocated, 1100u);
    CHECK_EQ(activity.deallocated, 2000u);
    CHECK_EQ(peakEventNewEventWait(*thread_state), PEAK_EVENT_WAIT);
    CHECK_EQ(peakEventPostponedEventWait(*thread_state), THREAD_EVENT_MIN_START_WAIT);
}

TEST(ThreadEvent, CounterAccumulated)
{
    CounterAccumulated counter;
    CHECK(!counter.init(100));
    CHECK(!counter.accumulate(nullptr, 30));
    CHECK_EQ(counter.accumulated_bytes.load(), 30u);
    CHECK(counter.accumulate(nullptr, 80));
    CHECK_EQ(counter.accumulated_bytes.load(), 10u);
    /// Extreme overflow coalesces triggers.
    CHECK(counter.accumulate(nullptr, 250));
    CHECK_EQ(counter.accumulated_bytes.load(), 60u);
    CHECK(counter.accumulate(nullptr, 40));
    CHECK_EQ(counter.accumulated_bytes.load(), 0u);
}
