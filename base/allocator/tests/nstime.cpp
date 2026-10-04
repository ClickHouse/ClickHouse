#include <allocator/NsTime.h>
#include <allocator/Options.h>

#include "Test.h"

#include <time.h>
#include <type_traits>

using namespace jemalloc;

/// Ported from jemalloc's `test/unit/nstime.c`.
TEST(NsTime, InitAndAccessors)
{
    NsTime t;
    t.init(42);
    CHECK_EQ(t.ns(), 42u);
    t.init2(42, 43);
    CHECK_EQ(t.ns(), 42 * NsTime::BILLION + 43);
    CHECK_EQ(t.sec(), 42u);
    CHECK_EQ(t.nsec(), 43u);
    CHECK_EQ(t.ms(), 42000u);
    t.init2(1, 999999999);
    CHECK_EQ(t.ms(), 1999u);
    CHECK_EQ(NsTime::zero().ns(), 0u);
    CHECK(NsTime::zero().equalsZero());
    CHECK(!t.equalsZero());
    t.initZero();
    CHECK(t.equalsZero());
    static_assert(NSTIME_SEC_MAX == 18446744072ULL);
    static_assert(sizeof(NsTime) == sizeof(uint64_t));
    static_assert(std::is_trivially_default_constructible_v<NsTime> && std::is_trivially_copyable_v<NsTime>);
}

TEST(NsTime, Compare)
{
    NsTime a;
    NsTime b;
    a.init2(42, 43);
    b.copy(a);
    CHECK_EQ(a.compare(b), 0);
    CHECK_EQ(b.compare(a), 0);
    b.init2(42, 42);
    CHECK_EQ(a.compare(b), 1);
    CHECK_EQ(b.compare(a), -1);
    b.init2(42, 44);
    CHECK_EQ(a.compare(b), -1);
    CHECK_EQ(b.compare(a), 1);
    b.init2(41, NsTime::BILLION - 1);
    CHECK_EQ(a.compare(b), 1);
    b.init2(43, 0);
    CHECK_EQ(a.compare(b), -1);
}

TEST(NsTime, Arithmetic)
{
    NsTime a;
    NsTime b;
    a.init2(42, 43);
    b.copy(a);
    a.add(b);
    CHECK_EQ(a.ns(), 84 * NsTime::BILLION + 86);
    a.init2(42, NsTime::BILLION - 1);
    b.copy(a);
    a.add(b);
    CHECK_EQ(a.sec(), 85u);
    CHECK_EQ(a.nsec(), NsTime::BILLION - 2);

    a.init2(42, 43);
    a.iadd(NsTime::BILLION - 1);
    CHECK_EQ(a.sec(), 43u);
    CHECK_EQ(a.nsec(), 42u);
    a.iadd(uint64_t(100) * NsTime::BILLION + 1);
    CHECK_EQ(a.sec(), 143u);
    CHECK_EQ(a.nsec(), 43u);

    a.init2(42, 43);
    b.copy(a);
    a.subtract(b);
    CHECK(a.equalsZero());
    a.init2(42, 43);
    b.init2(41, 44);
    a.subtract(b);
    CHECK_EQ(a.ns(), NsTime::BILLION - 1);

    a.init2(42, 43);
    a.isubtract(42 * NsTime::BILLION + 43);
    CHECK(a.equalsZero());
    a.init2(42, 43);
    a.isubtract(41 * NsTime::BILLION + 44);
    CHECK_EQ(a.ns(), NsTime::BILLION - 1);

    a.init2(42, 43);
    a.imultiply(10);
    CHECK_EQ(a.ns(), 420 * NsTime::BILLION + 430);
    a.init2(42, 666666666);
    a.imultiply(3);
    CHECK_EQ(a.ns(), 127 * NsTime::BILLION + 999999998);

    a.init2(42, 43);
    b.copy(a);
    a.imultiply(10);
    a.idivide(10);
    CHECK_EQ(a.compare(b), 0);
    a.init2(42, 666666666);
    b.copy(a);
    a.imultiply(3);
    a.idivide(3);
    CHECK_EQ(a.compare(b), 0);

    a.init2(42, 43);
    b.copy(a);
    a.imultiply(10);
    CHECK_EQ(a.divide(b), 10u);
    a.init2(42, 43);
    b.copy(a);
    a.imultiply(10);
    a.iadd(1);
    CHECK_EQ(a.divide(b), 10u);
    a.init2(42, 43);
    b.copy(a);
    a.imultiply(10);
    a.isubtract(1);
    CHECK_EQ(a.divide(b), 9u);

    a.init(1000);
    b.init(3500000);
    CHECK_EQ(NsTime::nsBetween(a, b), 3499000u);
    CHECK_EQ(NsTime::msBetween(a, b), 3u);
}

TEST(NsTime, Clock)
{
    static_assert(NsTime::isMonotonic() == !config::os_darwin);

    NsTime t;
    t.initUpdate();
    CHECK(!t.equalsZero());

    /// The clock is CLOCK_MONOTONIC (Linux, FreeBSD).
    if constexpr (config::have_clock_monotonic)
    {
        struct timespec before;
        clock_gettime(CLOCK_MONOTONIC, &before);
        NsTime now = NsTime::now();
        struct timespec after;
        clock_gettime(CLOCK_MONOTONIC, &after);
        CHECK_LE(uint64_t(before.tv_sec) * NsTime::BILLION + uint64_t(before.tv_nsec), now.ns());
        CHECK_GE(uint64_t(after.tv_sec) * NsTime::BILLION + uint64_t(after.tv_nsec), now.ns());
    }

    /// `update` never goes backwards relative to the current value.
    NsTime future;
    future.init(UINT64_MAX - 1);
    future.update();
    CHECK_EQ(future.ns(), UINT64_MAX - 1);

    NsTime past = NsTime::now();
    uint64_t since = past.nsSince();
    CHECK_LT(since, uint64_t(60) * NsTime::BILLION);
    CHECK_EQ(future.nsSince(), 0u);
    CHECK_EQ(future.msSince(), 0u);

    /// The profiling clock: CLOCK_REALTIME with `prof_time_resolution:high`, otherwise the regular clock, without
    /// the clamp.
    CHECK_STREQ(prof_time_res_mode_names[0], "default");
    CHECK_STREQ(prof_time_res_mode_names[1], "high");
    CHECK(opt.prof_time_res == ProfTimeRes::Default);

    NsTime prof = future;
    prof.profUpdate();
    CHECK_LT(prof.ns(), UINT64_MAX - 1);
    if constexpr (config::have_clock_monotonic)
    {
        struct timespec mono;
        clock_gettime(CLOCK_MONOTONIC, &mono);
        CHECK_LE(prof.ns(), uint64_t(mono.tv_sec) * NsTime::BILLION + uint64_t(mono.tv_nsec));
    }

    opt.prof_time_res = ProfTimeRes::High;
    struct timespec before;
    clock_gettime(CLOCK_REALTIME, &before);
    prof.profInitUpdate();
    struct timespec after;
    clock_gettime(CLOCK_REALTIME, &after);
    CHECK_LE(uint64_t(before.tv_sec) * NsTime::BILLION + uint64_t(before.tv_nsec), prof.ns());
    CHECK_GE(uint64_t(after.tv_sec) * NsTime::BILLION + uint64_t(after.tv_nsec), prof.ns());
    opt.prof_time_res = ProfTimeRes::Default;
}
