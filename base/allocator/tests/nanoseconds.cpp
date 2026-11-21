#include <allocator/Nanoseconds.h>
#include <allocator/Options.h>

#include "Test.h"

#include <type_traits>
#include <time.h>

using namespace jemalloc;

/// Ported from jemalloc's `test/unit/nstime.c`.
TEST(Nanoseconds, InitAndAccessors)
{
    Nanoseconds t;
    t.init(42);
    CHECK_EQ(t.ns(), 42u);
    t.initSecondsAndNanoseconds(42, 43);
    CHECK_EQ(t.ns(), 42 * Nanoseconds::BILLION + 43);
    CHECK_EQ(t.seconds(), 42u);
    CHECK_EQ(t.nsec(), 43u);
    CHECK_EQ(t.ms(), 42000u);
    t.initSecondsAndNanoseconds(1, 999999999);
    CHECK_EQ(t.ms(), 1999u);
    CHECK_EQ(Nanoseconds::zero().ns(), 0u);
    CHECK(Nanoseconds::zero().equalsZero());
    CHECK(!t.equalsZero());
    t.initZero();
    CHECK(t.equalsZero());
    static_assert(NANOSECONDS_MAX_SECONDS == 18446744072ULL);
    static_assert(sizeof(Nanoseconds) == sizeof(uint64_t));
    static_assert(std::is_trivially_default_constructible_v<Nanoseconds> && std::is_trivially_copyable_v<Nanoseconds>);
}

TEST(Nanoseconds, Compare)
{
    Nanoseconds a;
    Nanoseconds b;
    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    CHECK_EQ(a.compare(b), 0);
    CHECK_EQ(b.compare(a), 0);
    b.initSecondsAndNanoseconds(42, 42);
    CHECK_EQ(a.compare(b), 1);
    CHECK_EQ(b.compare(a), -1);
    b.initSecondsAndNanoseconds(42, 44);
    CHECK_EQ(a.compare(b), -1);
    CHECK_EQ(b.compare(a), 1);
    b.initSecondsAndNanoseconds(41, Nanoseconds::BILLION - 1);
    CHECK_EQ(a.compare(b), 1);
    b.initSecondsAndNanoseconds(43, 0);
    CHECK_EQ(a.compare(b), -1);
}

TEST(Nanoseconds, Arithmetic)
{
    Nanoseconds a;
    Nanoseconds b;
    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    a.add(b);
    CHECK_EQ(a.ns(), 84 * Nanoseconds::BILLION + 86);
    a.initSecondsAndNanoseconds(42, Nanoseconds::BILLION - 1);
    b.copy(a);
    a.add(b);
    CHECK_EQ(a.seconds(), 85u);
    CHECK_EQ(a.nsec(), Nanoseconds::BILLION - 2);

    a.initSecondsAndNanoseconds(42, 43);
    a.addNanoseconds(Nanoseconds::BILLION - 1);
    CHECK_EQ(a.seconds(), 43u);
    CHECK_EQ(a.nsec(), 42u);
    a.addNanoseconds(uint64_t(100) * Nanoseconds::BILLION + 1);
    CHECK_EQ(a.seconds(), 143u);
    CHECK_EQ(a.nsec(), 43u);

    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    a.subtract(b);
    CHECK(a.equalsZero());
    a.initSecondsAndNanoseconds(42, 43);
    b.initSecondsAndNanoseconds(41, 44);
    a.subtract(b);
    CHECK_EQ(a.ns(), Nanoseconds::BILLION - 1);

    a.initSecondsAndNanoseconds(42, 43);
    a.subtractNanoseconds(42 * Nanoseconds::BILLION + 43);
    CHECK(a.equalsZero());
    a.initSecondsAndNanoseconds(42, 43);
    a.subtractNanoseconds(41 * Nanoseconds::BILLION + 44);
    CHECK_EQ(a.ns(), Nanoseconds::BILLION - 1);

    a.initSecondsAndNanoseconds(42, 43);
    a.multiplyBy(10);
    CHECK_EQ(a.ns(), 420 * Nanoseconds::BILLION + 430);
    a.initSecondsAndNanoseconds(42, 666666666);
    a.multiplyBy(3);
    CHECK_EQ(a.ns(), 127 * Nanoseconds::BILLION + 999999998);

    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    a.multiplyBy(10);
    a.divideBy(10);
    CHECK_EQ(a.compare(b), 0);
    a.initSecondsAndNanoseconds(42, 666666666);
    b.copy(a);
    a.multiplyBy(3);
    a.divideBy(3);
    CHECK_EQ(a.compare(b), 0);

    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    a.multiplyBy(10);
    CHECK_EQ(a.divide(b), 10u);
    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    a.multiplyBy(10);
    a.addNanoseconds(1);
    CHECK_EQ(a.divide(b), 10u);
    a.initSecondsAndNanoseconds(42, 43);
    b.copy(a);
    a.multiplyBy(10);
    a.subtractNanoseconds(1);
    CHECK_EQ(a.divide(b), 9u);

    a.init(1000);
    b.init(3500000);
    CHECK_EQ(Nanoseconds::nsBetween(a, b), 3499000u);
    CHECK_EQ(Nanoseconds::msBetween(a, b), 3u);
}

TEST(Nanoseconds, Clock)
{
    static_assert(Nanoseconds::isMonotonic() == !config::os_darwin);

    Nanoseconds t;
    t.initUpdate();
    CHECK(!t.equalsZero());

    /// The clock is CLOCK_MONOTONIC (Linux, FreeBSD).
    if constexpr (config::have_clock_monotonic)
    {
        struct timespec before;
        clock_gettime(CLOCK_MONOTONIC, &before);
        Nanoseconds now = Nanoseconds::now();
        struct timespec after;
        clock_gettime(CLOCK_MONOTONIC, &after);
        CHECK_LE(uint64_t(before.tv_sec) * Nanoseconds::BILLION + uint64_t(before.tv_nsec), now.ns());
        CHECK_GE(uint64_t(after.tv_sec) * Nanoseconds::BILLION + uint64_t(after.tv_nsec), now.ns());
    }

    /// `update` never goes backwards relative to the current value.
    Nanoseconds future;
    future.init(UINT64_MAX - 1);
    future.update();
    CHECK_EQ(future.ns(), UINT64_MAX - 1);

    Nanoseconds past = Nanoseconds::now();
    uint64_t since = past.nsSince();
    CHECK_LT(since, uint64_t(60) * Nanoseconds::BILLION);
    CHECK_EQ(future.nsSince(), 0u);
    CHECK_EQ(future.msSince(), 0u);

    /// The profiling clock: CLOCK_REALTIME with `prof_time_resolution:high`, otherwise the regular clock, without
    /// the clamp.
    CHECK_STREQ(profiling_time_resolution_mode_names[0], "default");
    CHECK_STREQ(profiling_time_resolution_mode_names[1], "high");
    CHECK(options.profiling_time_resolution == ProfilingTimeResolution::Default);

    Nanoseconds profiling = future;
    profiling.profilingUpdate();
    CHECK_LT(profiling.ns(), UINT64_MAX - 1);
    if constexpr (config::have_clock_monotonic)
    {
        struct timespec monotonic;
        clock_gettime(CLOCK_MONOTONIC, &monotonic);
        CHECK_LE(profiling.ns(), uint64_t(monotonic.tv_sec) * Nanoseconds::BILLION + uint64_t(monotonic.tv_nsec));
    }

    options.profiling_time_resolution = ProfilingTimeResolution::High;
    struct timespec before;
    clock_gettime(CLOCK_REALTIME, &before);
    profiling.profilingInitUpdate();
    struct timespec after;
    clock_gettime(CLOCK_REALTIME, &after);
    CHECK_LE(uint64_t(before.tv_sec) * Nanoseconds::BILLION + uint64_t(before.tv_nsec), profiling.ns());
    CHECK_GE(uint64_t(after.tv_sec) * Nanoseconds::BILLION + uint64_t(after.tv_nsec), profiling.ns());
    options.profiling_time_resolution = ProfilingTimeResolution::Default;
}
