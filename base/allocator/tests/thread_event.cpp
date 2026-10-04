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
std::unique_ptr<ThreadState> makeNominalTsd(uint64_t prng_seed, uint64_t allocated = 0, uint64_t deallocated = 0)
{
    auto tsd = std::make_unique<ThreadState>();
    tsd->state.store(tsd_state_nominal, std::memory_order_relaxed);
    tsd->tcache_enabled = true;
    tsd->prng_state = prng_seed;
    tsd->thread_allocated = allocated;
    tsd->thread_deallocated = deallocated;
    tsdTeInit(*tsd);
    return tsd;
}

/// An independent model of jemalloc's event computation.
struct Model
{
    /// Waits indexed as `te_alloc_*` / `te_dalloc_*`.
    uint64_t alloc_wait[te_alloc_count] = {};
    uint64_t dalloc_wait[te_dalloc_count] = {};
    uint64_t current[2] = {};
    uint64_t last[2] = {};
    uint64_t next[2] = {};
    uint64_t prng = 0;
    bool nominal = true;
    int reentrancy = 0;
    std::vector<std::string> triggered;

    bool profOn() const { return opt.prof; }
    bool statsOn() const { return opt.stats_interval >= 0; }

    uint64_t newWait(int handler)
    {
        switch (handler)
        {
            case 0: return thread_test::profGeometricWait(prng, thread_test::lg_prof_sample);
            case 1: return stats_interval_accum_batch;
            case 2: return opt.tcache_gc_incr_bytes;
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
                bool enabled[4] = {profOn(), statsOn(), opt.tcache_gc_incr_bytes > 0, true};
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
                dalloc_wait[0] = newWait(2);
                dalloc_wait[1] = newWait(3);
                wait = std::min(dalloc_wait[0], dalloc_wait[1]);
            }
            next[a] = last[a] + std::min(wait, TE_MAX_INTERVAL);
        }
    }

    void update(uint64_t & ev_wait, uint64_t accum, bool allow, int handler, uint64_t new_wait, uint64_t & wait, const char * name)
    {
        if (ev_wait > accum)
            ev_wait -= accum;
        else if (!allow)
            ev_wait = postponedWait(handler);
        else
        {
            triggered.push_back(name);
            ev_wait = new_wait == 0 ? newWait(handler) : new_wait;
        }
        wait = std::min(wait, ev_wait);
    }

    void event(size_t usize, bool is_alloc)
    {
        int a = is_alloc ? 0 : 1;
        uint64_t before = current[a];
        current[a] += usize;
        if (usize < next[a] - before)
            return;
        uint64_t accum = current[a] - last[a];
        last[a] = current[a];
        bool allow = nominal && reentrancy == 0;
        uint64_t wait = UINT64_MAX;
        if (is_alloc)
        {
            update(alloc_wait[te_alloc_tcache_gc], accum, allow, 2, opt.tcache_gc_incr_bytes, wait, "tcacheGcEvent");
            if (profOn())
                update(alloc_wait[te_alloc_prof_sample], accum, allow, 0, 0, wait, "profSampleEvent");
            if (statsOn())
                update(alloc_wait[te_alloc_stats_interval], accum, allow, 1, stats_interval_accum_batch, wait, "statsIntervalEvent");
            update(alloc_wait[te_alloc_peak], accum, allow, 3, PEAK_EVENT_WAIT, wait, "peak");
        }
        else
        {
            update(dalloc_wait[te_dalloc_tcache_gc], accum, allow, 2, opt.tcache_gc_incr_bytes, wait, "tcacheGcEvent");
            update(dalloc_wait[te_dalloc_peak], accum, allow, 3, PEAK_EVENT_WAIT, wait, "peak");
        }
        next[a] = last[a] + std::min(wait, TE_MAX_INTERVAL);
    }
};

void compare(ThreadState & tsd, const Model & model, size_t step)
{
    int failures = allocator_test::failureCount();
    CHECK_EQ(tsd.thread_allocated, model.current[0]);
    CHECK_EQ(tsd.thread_allocated_last_event, model.last[0]);
    CHECK_EQ(tsd.thread_allocated_next_event, model.next[0]);
    CHECK_EQ(tsd.thread_deallocated, model.current[1]);
    CHECK_EQ(tsd.thread_deallocated_last_event, model.last[1]);
    CHECK_EQ(tsd.thread_deallocated_next_event, model.next[1]);
    for (unsigned i = 0; i < te_alloc_count; ++i)
        CHECK_EQ(tsd.te_data.alloc_wait[i], model.alloc_wait[i]);
    for (unsigned i = 0; i < te_dalloc_count; ++i)
        CHECK_EQ(tsd.te_data.dalloc_wait[i], model.dalloc_wait[i]);
    CHECK_EQ(tsd.prng_state, model.prng);
    bool fast = tsd.stateGet() == tsd_state_nominal;
    CHECK_EQ(tsd.thread_allocated_next_event_fast, fast && model.next[0] <= TE_NEXT_EVENT_FAST_MAX ? model.next[0] : 0);
    CHECK_EQ(tsd.thread_deallocated_next_event_fast, fast && model.next[1] <= TE_NEXT_EVENT_FAST_MAX ? model.next[1] : 0);
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
void runStream(uint64_t seed, size_t nsteps, uint64_t start_allocated, uint64_t start_deallocated)
{
    thread_test::takeLog();
    auto tsd = makeNominalTsd(seed, start_allocated, start_deallocated);
    Model model;
    model.prng = seed;
    model.init(start_allocated, start_deallocated);
    compare(*tsd, model, 0);
    CHECK(takeTriggered().empty());

    Rng rng{seed | 1};
    for (size_t step = 1; step <= nsteps; ++step)
    {
        uint64_t r = rng.next();
        if (r % 97 == 0)
        {
            /// Enter or leave reentrancy (non-nominal fast state; events are postponed).
            if (tsd->reentrancy_level == 0)
            {
                preReentrancy(*tsd, nullptr);
                CHECK_EQ(tsd->stateGet(), tsd_state_nominal_slow);
                model.reentrancy = 1;
            }
            else
            {
                postReentrancy(*tsd);
                CHECK_EQ(tsd->stateGet(), tsd_state_nominal);
                model.reentrancy = 0;
            }
            compare(*tsd, model, step);
            continue;
        }
        size_t usize;
        switch ((r >> 8) % 8)
        {
            case 0: usize = (r >> 16) % (8 << 20); break;
            case 1: usize = (r >> 16) % 300000; break;
            default: usize = 8 + (r >> 16) % 4096; break;
        }
        bool is_alloc = (r >> 12) % 3 != 0;
        if (is_alloc)
            threadAllocEvent(*tsd, usize);
        else
            threadDallocEvent(*tsd, usize);
        model.event(usize, is_alloc);
        compare(*tsd, model, step);
        CHECK(withoutPeak(model.triggered) == takeTriggered());
        model.triggered.clear();
    }
}

struct OptionsGuard
{
    Options saved = opt;
    unsigned saved_lg_prof_sample = thread_test::lg_prof_sample;
    uint64_t saved_batch = stats_interval_accum_batch;
    ~OptionsGuard()
    {
        opt = saved;
        thread_test::lg_prof_sample = saved_lg_prof_sample;
        stats_interval_accum_batch = saved_batch;
    }
};

}

TEST(ThreadEvent, Constants)
{
    CHECK_EQ(TE_MIN_START_WAIT, 1u);
    CHECK_EQ(TE_MAX_START_WAIT, UINT64_MAX);
    CHECK_EQ(TE_NEXT_EVENT_FAST_MAX, UINT64_MAX - 4096 + 1);
    CHECK_EQ(TE_MAX_INTERVAL, 4u << 20);
    CHECK_EQ(PEAK_EVENT_WAIT, 65536u);
    CHECK_EQ(unsigned(te_alloc_count), 8u);
    CHECK_EQ(unsigned(te_dalloc_count), 6u);
}

/// Default options (no prof, no stats interval): the only events are tcache GC and peak, both every 64 KiB.
TEST(ThreadEvent, DefaultSequence)
{
    OptionsGuard guard;
    thread_test::takeLog();
    auto tsd = makeNominalTsd(12345);
    CHECK_EQ(tsd->thread_allocated_last_event, 0u);
    CHECK_EQ(tsd->thread_allocated_next_event, 65536u);
    CHECK_EQ(tsd->thread_allocated_next_event_fast, 65536u);
    CHECK_EQ(tsd->thread_deallocated_next_event, 65536u);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_tcache_gc], 65536u);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_peak], 65536u);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_prof_sample], 0u);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_stats_interval], 0u);
    CHECK_EQ(tsd->te_data.dalloc_wait[te_dalloc_tcache_gc], 65536u);
    CHECK_EQ(tsd->te_data.dalloc_wait[te_dalloc_peak], 65536u);

    threadAllocEvent(*tsd, 65535);
    CHECK(takeTriggered().empty());
    CHECK_EQ(tsd->thread_allocated, 65535u);
    CHECK_EQ(tsd->thread_allocated_next_event, 65536u);

    /// No carry-over of the overshoot: the next waits start from the triggering byte count.
    threadAllocEvent(*tsd, 100);
    auto log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheGcEvent");
    CHECK_EQ(log[0].allocated, 65635u);
    CHECK_EQ(tsd->thread_allocated_last_event, 65635u);
    CHECK_EQ(tsd->thread_allocated_next_event, 65635u + 65536);
    CHECK_EQ(tsd->peak.cur_max, 65635u);

    threadDallocEvent(*tsd, 70000);
    log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheGcEvent");
    CHECK_EQ(tsd->thread_deallocated_next_event, 70000u + 65536);
    CHECK_EQ(tsd->peak.cur_max, 65635u);

    /// A huge allocation crosses several waits at once; it triggers each event once.
    threadAllocEvent(*tsd, 10 << 20);
    log = thread_test::takeLog();
    CHECK_EQ(log.size(), 1u);
    CHECK_EQ(tsd->thread_allocated_next_event, 65635u + (10 << 20) + 65536);
    CHECK_EQ(tsd->peak.cur_max, 65635u + (10 << 20) - 70000);
}

/// Without any event with a short wait the threshold is capped at `TE_MAX_INTERVAL`.
TEST(ThreadEvent, MaxInterval)
{
    OptionsGuard guard;
    opt.tcache_gc_incr_bytes = 100 << 20;
    auto tsd = makeNominalTsd(1);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_tcache_gc], uint64_t(100) << 20);
    CHECK_EQ(tsd->thread_allocated_next_event, PEAK_EVENT_WAIT);
    /// Peak triggers every 64 KiB, so the GC wait is decremented by the accumulated bytes.
    threadAllocEvent(*tsd, 65536);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_tcache_gc], (uint64_t(100) << 20) - 65536);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_peak], PEAK_EVENT_WAIT);
}

/// Events are postponed (wait 1) while the TSD is reentrant, and run at the next event after leaving.
TEST(ThreadEvent, Postponed)
{
    OptionsGuard guard;
    thread_test::takeLog();
    auto tsd = makeNominalTsd(7);
    preReentrancy(*tsd, nullptr);
    CHECK_EQ(tsd->thread_allocated_next_event_fast, 0u);
    CHECK_EQ(tsd->thread_deallocated_next_event_fast, 0u);
    threadAllocEvent(*tsd, 70000);
    CHECK(takeTriggered().empty());
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_tcache_gc], 1u);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_peak], 1u);
    CHECK_EQ(tsd->thread_allocated_next_event, 70001u);
    CHECK_EQ(tsd->thread_allocated_next_event_fast, 0u);
    postReentrancy(*tsd);
    CHECK_EQ(tsd->thread_allocated_next_event_fast, 70001u);
    threadAllocEvent(*tsd, 8);
    auto log = thread_test::takeLog();
    REQUIRE(log.size() == 1);
    CHECK(log[0].name == "tcacheGcEvent");
    CHECK_EQ(tsd->thread_allocated_next_event, 70008u + 65536);
}

/// With prof and stats interval on, the alloc handlers run in the order gc, prof, stats_interval, peak.
TEST(ThreadEvent, HandlerOrder)
{
    OptionsGuard guard;
    opt.prof = true;
    opt.stats_interval = 0;
    stats_interval_accum_batch = 1;
    thread_test::lg_prof_sample = 0; /// Wait 1: prof triggers on every event.
    thread_test::takeLog();
    auto tsd = makeNominalTsd(99);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_prof_sample], 1u);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_stats_interval], 1u);
    CHECK_EQ(tsd->thread_allocated_next_event, 1u);
    threadAllocEvent(*tsd, 65536);
    auto names = takeTriggered();
    CHECK((names == std::vector<std::string>{"tcacheGcEvent", "profSampleEvent", "statsIntervalEvent"}));
    CHECK_EQ(tsd->peak.cur_max, 65536u);
    threadAllocEvent(*tsd, 16);
    names = takeTriggered();
    CHECK((names == std::vector<std::string>{"profSampleEvent", "statsIntervalEvent"}));
}

/// The prof sample waits are drawn from the TSD's PRNG, in table order at init (prof first).
TEST(ThreadEvent, ProfWaits)
{
    OptionsGuard guard;
    opt.prof = true;
    thread_test::lg_prof_sample = 19;
    uint64_t seed = 0x123456789;
    auto tsd = makeNominalTsd(seed);
    uint64_t expected_prng = seed;
    uint64_t expected_wait = thread_test::profGeometricWait(expected_prng, 19);
    CHECK_EQ(tsd->te_data.alloc_wait[te_alloc_prof_sample], expected_wait);
    CHECK_EQ(tsd->prng_state, expected_prng);
    CHECK_EQ(tsd->thread_allocated_next_event, std::min(expected_wait, PEAK_EVENT_WAIT));
}

TEST(ThreadEvent, RandomStreamsDefault)
{
    OptionsGuard guard;
    for (uint64_t seed = 1; seed <= 10; ++seed)
        runStream(seed * 0x9e3779b97f4a7c15ULL, 20000, 0, 0);
}

TEST(ThreadEvent, RandomStreamsProf)
{
    OptionsGuard guard;
    opt.prof = true;
    for (unsigned lg : {19u, 12u, 0u})
    {
        thread_test::lg_prof_sample = lg;
        for (uint64_t seed = 1; seed <= 5; ++seed)
            runStream(seed * 0x2545f4914f6cdd1dULL + lg, 20000, 0, 0);
    }
}

TEST(ThreadEvent, RandomStreamsStatsInterval)
{
    OptionsGuard guard;
    opt.prof = true;
    opt.stats_interval = 1 << 20;
    stats_interval_accum_batch = (1 << 20) >> 6;
    opt.tcache_gc_incr_bytes = 1024;
    for (uint64_t seed = 1; seed <= 5; ++seed)
        runStream(seed * 0x5851f42d4c957f2dULL, 20000, 0, 0);
}

/// Counters near the 64-bit wrap: the fast threshold is 0 while `next_event > TE_NEXT_EVENT_FAST_MAX`.
TEST(ThreadEvent, Wraparound)
{
    OptionsGuard guard;
    auto tsd = makeNominalTsd(5, UINT64_MAX - 67000, UINT64_MAX - 10);
    CHECK_EQ(tsd->thread_allocated_next_event, UINT64_MAX - 67000 + 65536);
    CHECK_EQ(tsd->thread_allocated_next_event_fast, 0u);
    CHECK_EQ(tsd->thread_deallocated_next_event, uint64_t(65536 - 11));
    CHECK_EQ(tsd->thread_deallocated_next_event_fast, uint64_t(65536 - 11));
    for (uint64_t seed = 1; seed <= 5; ++seed)
        runStream(seed * 0x9e3779b97f4a7c15ULL, 5000, UINT64_MAX - seed * 50000, UINT64_MAX - seed * 3000);
}

TEST(ThreadEvent, Peak)
{
    OptionsGuard guard;
    auto tsd = makeNominalTsd(3);
    tsd->thread_allocated = 1000;
    tsd->thread_deallocated = 200;
    peakEventUpdate(*tsd);
    CHECK_EQ(peakEventMax(*tsd), 800u);
    tsd->thread_deallocated = 900;
    peakEventUpdate(*tsd);
    CHECK_EQ(peakEventMax(*tsd), 800u);
    peakEventZero(*tsd);
    CHECK_EQ(peakEventMax(*tsd), 0u);
    CHECK_EQ(tsd->peak.adjustment, 100u);
    tsd->thread_allocated = 1100;
    peakEventUpdate(*tsd);
    CHECK_EQ(peakEventMax(*tsd), 100u);
    /// A negative candidate (more deallocated than allocated since the reset) does not lower the peak.
    tsd->thread_deallocated = 2000;
    peakEventUpdate(*tsd);
    CHECK_EQ(peakEventMax(*tsd), 100u);

    struct Activity
    {
        int calls = 0;
        uint64_t allocated = 0;
        uint64_t deallocated = 0;
    } activity;
    tsd->activity_callback_thunk.callback = [](void * uctx, uint64_t allocated, uint64_t deallocated)
    {
        auto * a = static_cast<Activity *>(uctx);
        ++a->calls;
        a->allocated = allocated;
        a->deallocated = deallocated;
    };
    tsd->activity_callback_thunk.uctx = &activity;
    peakEvent(*tsd);
    CHECK_EQ(activity.calls, 1);
    CHECK_EQ(activity.allocated, 1100u);
    CHECK_EQ(activity.deallocated, 2000u);
    CHECK_EQ(peakEventNewEventWait(*tsd), PEAK_EVENT_WAIT);
    CHECK_EQ(peakEventPostponedEventWait(*tsd), TE_MIN_START_WAIT);
}

TEST(ThreadEvent, CounterAccum)
{
    CounterAccum counter;
    CHECK(!counter.init(100));
    CHECK(!counter.accum(nullptr, 30));
    CHECK_EQ(counter.accumbytes.load(), 30u);
    CHECK(counter.accum(nullptr, 80));
    CHECK_EQ(counter.accumbytes.load(), 10u);
    /// Extreme overflow coalesces triggers.
    CHECK(counter.accum(nullptr, 250));
    CHECK_EQ(counter.accumbytes.load(), 60u);
    CHECK(counter.accum(nullptr, 40));
    CHECK_EQ(counter.accumbytes.load(), 0u);
}
