/// Unit tests of `Decay` and the smoothstep table: the assertions of jemalloc's `test/unit/decay.c` and
/// `test/unit/smoothstep.c`, plus pinned values.

#include <allocator/Decay.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <new>

using namespace jemalloc;

namespace
{

alignas(64) unsigned char storage[sizeof(Decay)];

}

/// --- The assertions of jemalloc's test/unit/decay.c ---------------------------------------------------------------

namespace
{

struct DecayHolder
{
    Decay * decay;
    DecayHolder()
    {
        std::memset(storage, 0, sizeof(Decay));
        decay = new (storage) Decay;
    }
    ~DecayHolder() { decay->~Decay(); }
    Decay * operator->() { return decay; }
};

}

TEST(Decay, Init)
{
    DecayHolder decay;
    ssize_t decay_ms = 1000;
    REQUIRE(Decay::msValid(decay_ms));
    CHECK(!decay->init(NsTime::fromNs(0), decay_ms));
    CHECK_EQ(decay->msRead(), decay_ms);
    CHECK_NE(decay->epochDurationNs(), 0u);
}

TEST(Decay, MsValid)
{
    CHECK(!Decay::msValid(-7));
    CHECK(Decay::msValid(-1));
    CHECK(Decay::msValid(8943));
    CHECK(!Decay::msValid(ssize_t(NSTIME_SEC_MAX * 1000 + 39)));
}

TEST(Decay, NpagesPurgeIn)
{
    DecayHolder decay;
    uint64_t decay_ms = 1000;
    NsTime decay_nstime = NsTime::fromNs(decay_ms * 1000 * 1000);
    CHECK(!decay->init(NsTime::fromNs(0), ssize_t(decay_ms)));

    size_t new_pages = 100;

    NsTime time = decay_nstime;
    CHECK_EQ(decay->npagesPurgeIn(time, new_pages), uint64_t(new_pages));

    time.init(0);
    CHECK_EQ(decay->npagesPurgeIn(time, new_pages), 0u);

    time = decay_nstime;
    time.idivide(2);
    CHECK_EQ(decay->npagesPurgeIn(time, new_pages), uint64_t(new_pages / 2));
}

TEST(Decay, MaybeAdvanceEpoch)
{
    DecayHolder decay;
    NsTime curtime = NsTime::fromNs(0);
    CHECK(!decay->init(curtime, 1000));

    CHECK(!decay->maybeAdvanceEpoch(curtime, 0));

    NsTime interval = NsTime::fromNs(decay->epochDurationNs());
    curtime.add(interval);
    CHECK(!decay->maybeAdvanceEpoch(curtime, 0));

    curtime.add(interval);
    CHECK(decay->maybeAdvanceEpoch(curtime, 0));
}

TEST(Decay, Empty)
{
    DecayHolder decay;
    uint64_t decay_ms = 1000;
    uint64_t decay_ns = decay_ms * 1000 * 1000;
    REQUIRE(!decay->init(NsTime::fromNs(0), ssize_t(decay_ms)));

    uint64_t time_between_calls = decay->epochDurationNs() / 5;
    int nepochs = 0;
    for (uint64_t i = 0; i < decay_ns / time_between_calls * 10; ++i)
    {
        if (decay->maybeAdvanceEpoch(NsTime::fromNs(i * time_between_calls), 0))
        {
            ++nepochs;
            CHECK_EQ(decay->npagesLimitGet(), 0u);
        }
    }
    CHECK_GT(nepochs, 0);
}

TEST(Decay, Decay)
{
    const uint64_t nepoch_init = 10;
    DecayHolder decay;
    NsTime curtime = NsTime::fromNs(0);
    uint64_t decay_ms = 1000;
    uint64_t decay_ns = decay_ms * 1000 * 1000;
    REQUIRE(!decay->init(curtime, ssize_t(decay_ms)));
    CHECK_EQ(decay->npagesLimitGet(), 0u);

    NsTime epochtime = NsTime::fromNs(decay->epochDurationNs());
    const size_t dirty_pages_per_epoch = 1000;
    size_t dirty_pages = 0;
    uint64_t epoch_ns = decay->epochDurationNs();
    bool epoch_advanced = false;

    for (uint64_t i = 0; i < nepoch_init; ++i)
    {
        curtime.add(epochtime);
        dirty_pages += dirty_pages_per_epoch;
        epoch_advanced |= decay->maybeAdvanceEpoch(curtime, dirty_pages);
    }
    CHECK(epoch_advanced);

    size_t npages_limit = decay->npagesLimitGet();
    CHECK_GT(npages_limit, 0u);

    for (uint64_t i = nepoch_init; i * epoch_ns < decay_ns; ++i)
    {
        curtime.add(epochtime);
        if (decay->maybeAdvanceEpoch(curtime, dirty_pages))
        {
            size_t npages_limit_new = decay->npagesLimitGet();
            CHECK_LT(npages_limit_new, npages_limit);
            npages_limit = npages_limit_new;
        }
    }
    CHECK_GT(npages_limit, 0u);

    epoch_advanced = false;
    for (uint64_t i = 0; i < nepoch_init; ++i)
    {
        curtime.add(epochtime);
        epoch_advanced |= decay->maybeAdvanceEpoch(curtime, dirty_pages);
    }
    CHECK(epoch_advanced);
    CHECK_EQ(decay->npagesLimitGet(), 0u);
}

TEST(Decay, NsUntilPurge)
{
    const uint64_t nepoch_init = 10;
    DecayHolder decay;
    NsTime curtime = NsTime::fromNs(0);
    uint64_t decay_ms = 1000;
    uint64_t decay_ns = decay_ms * 1000 * 1000;
    REQUIRE(!decay->init(curtime, ssize_t(decay_ms)));

    NsTime epochtime = NsTime::fromNs(decay->epochDurationNs());
    CHECK_EQ(decay->nsUntilPurge(0, 0), DECAY_UNBOUNDED_TIME_TO_PURGE);

    const size_t dirty_pages_per_epoch = 1000;
    size_t dirty_pages = 0;
    bool epoch_advanced = false;
    for (uint64_t i = 0; i < nepoch_init; ++i)
    {
        curtime.add(epochtime);
        dirty_pages += dirty_pages_per_epoch;
        epoch_advanced |= decay->maybeAdvanceEpoch(curtime, dirty_pages);
    }
    CHECK(epoch_advanced);

    CHECK_GE(decay->nsUntilPurge(dirty_pages, dirty_pages), decay_ns);
    CHECK_EQ(decay->nsUntilPurge(dirty_pages, 0), decay->epochDurationNs() * 2);

    uint64_t npages_threshold = dirty_pages / 2;
    uint64_t ns_until_purge_half = decay->nsUntilPurge(dirty_pages, npages_threshold);

    curtime.add(NsTime::fromNs(ns_until_purge_half));
    decay->maybeAdvanceEpoch(curtime, dirty_pages);
    size_t npages_limit = decay->npagesLimitGet();
    CHECK_LT(npages_limit, dirty_pages);
    size_t expected = dirty_pages - npages_limit;
    int deviation = std::abs(int(expected) - int(npages_threshold));
    CHECK_LT(deviation, int(npages_threshold / 2));
}

/// --- The assertions of jemalloc's test/unit/smoothstep.c ----------------------------------------------------------

TEST(Smoothstep, Integral)
{
    /// The integral of smoothstep in the [0..1] range equals 1/2; each table element is rounded down, so the
    /// integral may be off by as much as SMOOTHSTEP_NSTEPS ulps.
    uint64_t sum = 0;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
        sum += smoothstep_h_steps[i];
    uint64_t max = (uint64_t(1) << (SMOOTHSTEP_BFP - 1)) * (SMOOTHSTEP_NSTEPS + 1);
    uint64_t min = max - SMOOTHSTEP_NSTEPS;
    CHECK_GE(sum, min);
    CHECK_LE(sum, max);
}

TEST(Smoothstep, Monotonic)
{
    uint64_t prev_h = 0;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; ++i)
    {
        uint64_t h = smoothstep_h_steps[i];
        CHECK_GE(h, prev_h);
        prev_h = h;
    }
    CHECK_EQ(smoothstep_h_steps[SMOOTHSTEP_NSTEPS - 1], uint64_t(1) << SMOOTHSTEP_BFP);
}

TEST(Smoothstep, Slope)
{
    uint64_t prev_h = 0;
    uint64_t prev_delta = 0;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS / 2 + SMOOTHSTEP_NSTEPS % 2; ++i)
    {
        uint64_t h = smoothstep_h_steps[i];
        uint64_t delta = h - prev_h;
        CHECK_GE(delta, prev_delta);
        prev_h = h;
        prev_delta = delta;
    }

    prev_h = uint64_t(1) << SMOOTHSTEP_BFP;
    prev_delta = 0;
    for (unsigned i = SMOOTHSTEP_NSTEPS - 1; i >= SMOOTHSTEP_NSTEPS / 2; --i)
    {
        uint64_t h = smoothstep_h_steps[i];
        uint64_t delta = prev_h - h;
        CHECK_GE(delta, prev_delta);
        prev_h = h;
        prev_delta = delta;
    }
}
