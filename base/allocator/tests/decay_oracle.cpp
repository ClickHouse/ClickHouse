/// Compares `Decay` with jemalloc's `decay_t` (`decay.c`) under scripted time sequences: epoch advances, npages
/// limits, backlogs, deadlines (including the jitter stream seeded with the object address - both implementations run
/// on the same memory), `ns_until_purge` and `npages_purge_in` results.

#include <allocator/Decay.h>

#include "Test.h"
#include "decay_oracle_ref.h"

#include <cstddef>
#include <cstring>
#include <iterator>
#include <new>
#include <random>
#include <vector>

using namespace jemalloc;

extern "C"
{
/// `decay.o` (through `malloc_mutex_init`) pulls in the rest of the reference jemalloc, including the libunwind-based
/// profiler backtrace, which is never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

namespace
{

alignas(64) unsigned char storage[sizeof(Decay) + 64];

enum class OpKind
{
    Init,
    Reinit,
    Advance,
    UntilPurge,
    PurgeIn,
    Queries,
};

struct Op
{
    OpKind kind;
    uint64_t a = 0;
    uint64_t b = 0;
    int64_t ms = 0;
};

/// The C implementation.
struct RefImpl
{
    void * mem;
    explicit RefImpl(size_t offset)
        : mem(storage + offset)
    {
        std::memset(mem, 0, sizeof(Decay));
    }
    bool init(uint64_t ns, ssize_t ms) { return ref_decay_init(mem, ns, ms); }
    void reinit(uint64_t ns, ssize_t ms) { ref_decay_reinit(mem, ns, ms); }
    bool advance(uint64_t ns, size_t npages) { return ref_decay_maybe_advance_epoch(mem, ns, npages); }
    uint64_t untilPurge(size_t npages, uint64_t threshold) { return ref_decay_ns_until_purge(mem, npages, threshold); }
    uint64_t purgeIn(uint64_t ns, size_t npages) { return ref_decay_npages_purge_in(mem, ns, npages); }
    uint64_t queries()
    {
        return uint64_t(ref_decay_queries(mem, 0)) | (uint64_t(ref_decay_queries(mem, 1)) << 1)
            | (uint64_t(ref_decay_queries(mem, 2)) << 2) | (uint64_t(ref_decay_queries(mem, 3)) << 3);
    }
    void state(RefDecayState & s) { ref_decay_state(mem, &s); }
};

/// The C++ implementation, at the same address.
struct NewImpl
{
    Decay * decay;
    explicit NewImpl(size_t offset)
    {
        std::memset(storage + offset, 0, sizeof(Decay));
        decay = new (storage + offset) Decay;
    }
    ~NewImpl() { decay->~Decay(); }
    bool init(uint64_t ns, ssize_t ms) { return decay->init(NsTime::fromNs(ns), ms); }
    void reinit(uint64_t ns, ssize_t ms) { decay->reinit(NsTime::fromNs(ns), ms); }
    bool advance(uint64_t ns, size_t npages) { return decay->maybeAdvanceEpoch(NsTime::fromNs(ns), npages); }
    uint64_t untilPurge(size_t npages, uint64_t threshold) { return decay->nsUntilPurge(npages, threshold); }
    uint64_t purgeIn(uint64_t ns, size_t npages) { return decay->npagesPurgeIn(NsTime::fromNs(ns), npages); }
    uint64_t queries()
    {
        return uint64_t(decay->immediately()) | (uint64_t(decay->disabled()) << 1) | (uint64_t(decay->gradually()) << 2)
            | (uint64_t(decay->epochNpagesDelta() != 0) << 3);
    }
    void state(RefDecayState & s)
    {
        s.time_ms = decay->msRead();
        s.interval = decay->epochDurationNs();
        s.epoch = decay->epoch.ns();
        s.jitter_state = decay->jitter_state;
        s.deadline = decay->deadline.ns();
        s.npages_limit = decay->npagesLimitGet();
        s.nunpurged = decay->nunpurged;
        for (size_t i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
            s.backlog[i] = decay->backlog[i];
        s.purging = decay->purging;
    }
};

void pushState(std::vector<uint64_t> & trace, const RefDecayState & s)
{
    trace.push_back(uint64_t(s.time_ms));
    trace.push_back(s.interval);
    trace.push_back(s.epoch);
    trace.push_back(s.jitter_state);
    trace.push_back(s.deadline);
    trace.push_back(s.npages_limit);
    trace.push_back(s.nunpurged);
    for (unsigned i = 0; i < REF_DECAY_NSTEPS; ++i)
        trace.push_back(s.backlog[i]);
    trace.push_back(s.purging);
}

template <typename Impl>
std::vector<uint64_t> run(const std::vector<Op> & ops, size_t offset)
{
    std::vector<uint64_t> trace;
    Impl impl(offset);
    RefDecayState s;
    for (const Op & op : ops)
    {
        switch (op.kind)
        {
            case OpKind::Init:
                trace.push_back(impl.init(op.a, op.ms));
                break;
            case OpKind::Reinit:
                impl.reinit(op.a, op.ms);
                break;
            case OpKind::Advance:
                trace.push_back(impl.advance(op.a, op.b));
                break;
            case OpKind::UntilPurge:
                trace.push_back(impl.untilPurge(op.a, op.b));
                break;
            case OpKind::PurgeIn:
                trace.push_back(impl.purgeIn(op.a, op.b));
                break;
            case OpKind::Queries:
                trace.push_back(impl.queries());
                break;
        }
        impl.state(s);
        pushState(trace, s);
    }
    return trace;
}

int64_t pickMs(std::mt19937_64 & rng, bool allow_non_positive)
{
    static constexpr int64_t choices[] = {1, 2, 7, 10, 100, 1000, 5000, 10000, 60000, 1000000};
    unsigned r = rng() % 100;
    if (allow_non_positive && r < 8)
        return (r & 1) ? 0 : -1;
    if (r < 70)
        return choices[rng() % std::size(choices)];
    return int64_t(1 + rng() % 10000000);
}

std::vector<Op> generate(uint64_t seed, size_t nops)
{
    std::mt19937_64 rng(seed);
    std::vector<Op> ops;

    uint64_t t = (rng() % 4 == 0) ? 0 : rng() % 1000000000000ULL;
    int64_t ms = pickMs(rng, seed % 5 == 0);
    ops.push_back({OpKind::Init, t, 0, ms});
    uint64_t interval = ms > 0 ? uint64_t(ms) * 1000000 / 200 : 0;

    uint64_t npages = 0;
    for (size_t i = 0; i < nops; ++i)
    {
        /// Random walk of the number of pages.
        unsigned np = rng() % 100;
        if (np < 40)
            npages += rng() % 1000;
        else if (np < 60)
            npages -= npages ? rng() % npages : 0;
        else if (np < 63)
            npages = 0;
        else if (np < 65)
            npages = rng() % (uint64_t(1) << 30);

        unsigned r = rng() % 100;
        if (r < 55 && interval != 0)
        {
            unsigned dt = rng() % 10;
            uint64_t delta = 0;
            if (dt == 0)
                delta = 0;
            else if (dt < 4)
                delta = rng() % interval;
            else if (dt < 8)
                delta = interval * (1 + rng() % 3) + rng() % (interval / 2 + 1) - interval / 4;
            else if (dt == 8)
                delta = interval * (rng() % 400);
            else
                delta = rng() % (interval * 1000 + 1);
            t += delta;
            ops.push_back({OpKind::Advance, t, npages, 0});
        }
        else if (r < 72)
        {
            uint64_t threshold;
            switch (rng() % 6)
            {
                case 0: threshold = 0; break;
                case 1: threshold = npages; break;
                case 2: threshold = npages / 2; break;
                case 3: threshold = npages * 2; break;
                case 4: threshold = rng() % 1024; break;
                default: threshold = rng() % (npages + 1); break;
            }
            ops.push_back({OpKind::UntilPurge, npages, threshold, 0});
        }
        else if (r < 85 && interval != 0)
        {
            uint64_t time_ns = rng() % (interval * 200 * 2 + 1);
            ops.push_back({OpKind::PurgeIn, time_ns, npages, 0});
        }
        else if (r < 88)
        {
            t += rng() % 1000000;
            ms = pickMs(rng, true);
            if (ms > 0)
                interval = uint64_t(ms) * 1000000 / 200;
            ops.push_back({OpKind::Reinit, t, 0, ms});
        }
        else
        {
            ops.push_back({OpKind::Queries, 0, 0, 0});
        }
    }
    return ops;
}

void compareScript(uint64_t seed, size_t nops, size_t offset)
{
    std::vector<Op> ops = generate(seed, nops);
    std::vector<uint64_t> expected = run<RefImpl>(ops, offset);
    std::vector<uint64_t> actual = run<NewImpl>(ops, offset);
    CHECK_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size() && i < actual.size(); ++i)
    {
        if (expected[i] != actual[i])
        {
            std::fprintf(stderr, "seed %llu: mismatch at trace position %zu\n", static_cast<unsigned long long>(seed), i);
            CHECK_EQ(expected[i], actual[i]);
            break;
        }
    }
}

}

TEST(DecayOracle, Layout)
{
    size_t layout[REF_DECAY_LAYOUT_SIZE];
    ref_decay_layout(layout);
    CHECK_EQ(layout[REF_DECAY_SIZEOF], sizeof(Decay));
    CHECK_EQ(layout[REF_DECAY_OFFSET_PURGING], offsetof(Decay, purging));
    CHECK_EQ(layout[REF_DECAY_OFFSET_TIME_MS], offsetof(Decay, time_ms));
    CHECK_EQ(layout[REF_DECAY_OFFSET_INTERVAL], offsetof(Decay, interval));
    CHECK_EQ(layout[REF_DECAY_OFFSET_EPOCH], offsetof(Decay, epoch));
    CHECK_EQ(layout[REF_DECAY_OFFSET_JITTER_STATE], offsetof(Decay, jitter_state));
    CHECK_EQ(layout[REF_DECAY_OFFSET_DEADLINE], offsetof(Decay, deadline));
    CHECK_EQ(layout[REF_DECAY_OFFSET_NPAGES_LIMIT], offsetof(Decay, npages_limit));
    CHECK_EQ(layout[REF_DECAY_OFFSET_NUNPURGED], offsetof(Decay, nunpurged));
    CHECK_EQ(layout[REF_DECAY_OFFSET_BACKLOG], offsetof(Decay, backlog));
    CHECK_EQ(layout[REF_DECAY_OFFSET_CEIL_NPAGES], offsetof(Decay, ceil_npages));
}

TEST(DecayOracle, SmoothstepTable)
{
    CHECK_EQ(size_t(ref_smoothstep_nsteps()), SMOOTHSTEP_NSTEPS);
    CHECK_EQ(ref_smoothstep_bfp(), SMOOTHSTEP_BFP);
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
        CHECK_EQ(ref_h_step(i), smoothstep_h_steps[i]);
}

TEST(DecayOracle, MsValid)
{
    static constexpr int64_t values[] = {
        INT64_MIN, -1000, -7, -2, -1, 0, 1, 8943, 1000000,
        int64_t(NSTIME_SEC_MAX * 1000) - 1, int64_t(NSTIME_SEC_MAX * 1000), int64_t(NSTIME_SEC_MAX * 1000) + 1,
        int64_t(NSTIME_SEC_MAX * 1000) + 39, INT64_MAX};
    for (int64_t v : values)
        CHECK_EQ(ref_decay_ms_valid(v), Decay::msValid(v));
}

TEST(DecayOracle, Scripts)
{
    for (uint64_t seed = 1; seed <= 300; ++seed)
        compareScript(seed, 1500, (seed % 8) * 8);
}

TEST(DecayOracle, LongScripts)
{
    for (uint64_t seed = 1000; seed < 1010; ++seed)
        compareScript(seed, 30000, 0);
}

/// Every step of the (5000 ms) dirty decay of ClickHouse's configuration, with steady page growth then a long idle period.
TEST(DecayOracle, ClickHouseDirtyDecay)
{
    std::vector<Op> ops;
    uint64_t t = 123456789;
    ops.push_back({OpKind::Init, t, 0, 5000});
    size_t npages = 0;
    for (size_t i = 0; i < 2000; ++i)
    {
        t += 7000000 + (i % 13) * 1000000;
        if (i < 500)
            npages += 37;
        else if (i > 1500)
            npages = npages > 50 ? npages - 50 : 0;
        ops.push_back({OpKind::Advance, t, npages, 0});
        ops.push_back({OpKind::UntilPurge, npages, 1024, 0});
        ops.push_back({OpKind::UntilPurge, npages, 0, 0});
    }
    std::vector<uint64_t> expected = run<RefImpl>(ops, 0);
    std::vector<uint64_t> actual = run<NewImpl>(ops, 0);
    CHECK(expected == actual);
}
