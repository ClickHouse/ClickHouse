#pragma once

/// Nanosecond time values and the allocator's clock.
/// jemalloc: `nstime.h`, `src/nstime.c`.
///
/// The clock (`nstime_get`): `CLOCK_MONOTONIC` on Linux and FreeBSD (`JEMALLOC_HAVE_CLOCK_MONOTONIC_COARSE` is not
/// defined), `gettimeofday` on Darwin (the ClickHouse fork disables `CLOCK_MONOTONIC` and `mach_absolute_time` there,
/// so that the clock matches the one used by the background thread's condition variable); `isMonotonic()` reports
/// which one. The debug-only `magic` field of `nstime_t` is not reproduced (jemalloc is built without
/// `JEMALLOC_DEBUG` in ClickHouse), so `Nanoseconds` has the size of `nstime_t` in that configuration.

#include <allocator/Common.h>

#include <cstdint>

namespace jemalloc
{

/// Maximum supported number of seconds (~584 years) (`NANOSECONDS_MAX_SECONDS`).
inline constexpr uint64_t NANOSECONDS_MAX_SECONDS = 18446744072ULL;

/// jemalloc: prof_time_res_t
enum class ProfilingTimeResolution : unsigned
{
    Default = 0,
    High = 1,
};

/// The `prof_time_resolution` option (`opt.prof_time_res`) and `profiling_time_resolution_mode_names` are in Options.h.

/// jemalloc: nstime_t
///
/// A trivial type (zero-filled memory is a valid zero time), like the C struct.
class Nanoseconds
{
public:
    /// jemalloc: nstime_zero
    static constexpr Nanoseconds zero()
    {
        Nanoseconds t;
        t.value = 0;
        return t;
    }

    static constexpr Nanoseconds fromNanoseconds(uint64_t ns)
    {
        Nanoseconds t;
        t.value = ns;
        return t;
    }

    /// jemalloc: nstime_init
    void init(uint64_t ns) { value = ns; }

    /// jemalloc: nstime_init2
    void initSecondsAndNanoseconds(uint64_t seconds, uint64_t nsec) { value = seconds * BILLION + nsec; }

    /// jemalloc: nstime_init_zero
    void initZero() { value = 0; }

    /// jemalloc: nstime_ns
    uint64_t ns() const { return value; }

    /// jemalloc: nstime_ms
    uint64_t ms() const { return value / MILLION; }

    /// jemalloc: nstime_sec
    uint64_t seconds() const { return value / BILLION; }

    /// jemalloc: nstime_nsec
    uint64_t nsec() const { return value % BILLION; }

    /// jemalloc: nstime_copy
    void copy(const Nanoseconds & source) { value = source.value; }

    /// Returns -1, 0 or 1.
    /// jemalloc: nstime_compare
    int compare(const Nanoseconds & other) const { return (value > other.value) - (value < other.value); }

    /// jemalloc: nstime_equals_zero
    bool equalsZero() const { return value == 0; }

    /// jemalloc: nstime_add
    void add(const Nanoseconds & addend)
    {
        ALLOCATOR_ASSERT(UINT64_MAX - value >= addend.value);
        value += addend.value;
    }

    /// jemalloc: nstime_iadd
    void addNanoseconds(uint64_t addend)
    {
        ALLOCATOR_ASSERT(UINT64_MAX - value >= addend);
        value += addend;
    }

    /// jemalloc: nstime_subtract
    void subtract(const Nanoseconds & subtrahend)
    {
        ALLOCATOR_ASSERT(compare(subtrahend) >= 0);
        value -= subtrahend.value;
    }

    /// jemalloc: nstime_isubtract
    void subtractNanoseconds(uint64_t subtrahend)
    {
        ALLOCATOR_ASSERT(value >= subtrahend);
        value -= subtrahend;
    }

    /// jemalloc: nstime_imultiply
    void multiplyBy(uint64_t multiplier)
    {
        ALLOCATOR_ASSERT(
            (((value | multiplier) & (UINT64_MAX << (sizeof(uint64_t) << 2))) == 0) || ((value * multiplier) / multiplier == value));
        value *= multiplier;
    }

    /// jemalloc: nstime_idivide
    void divideBy(uint64_t divisor)
    {
        ALLOCATOR_ASSERT(divisor != 0);
        value /= divisor;
    }

    /// jemalloc: nstime_divide
    uint64_t divide(const Nanoseconds & divisor) const
    {
        ALLOCATOR_ASSERT(divisor.value != 0);
        return value / divisor.value;
    }

    /// jemalloc: nstime_ns_between
    static uint64_t nsBetween(const Nanoseconds & earlier, const Nanoseconds & later)
    {
        ALLOCATOR_ASSERT(later.compare(earlier) >= 0);
        return later.value - earlier.value;
    }

    /// jemalloc: nstime_ms_between
    static uint64_t msBetween(const Nanoseconds & earlier, const Nanoseconds & later) { return nsBetween(earlier, later) / MILLION; }

    /// Time since `*this` in nanoseconds, without updating `*this`.
    /// jemalloc: nstime_ns_since
    uint64_t nsSince() const;

    /// jemalloc: nstime_ms_since
    uint64_t msSince() const { return nsSince() / MILLION; }

    /// Whether the clock is monotonic.
    /// jemalloc: nstime_monotonic
    static constexpr bool isMonotonic() { return config::have_clock_monotonic; }

    /// Read the clock; if the clock went backwards relative to the current value, keep the current value.
    /// jemalloc: nstime_update
    void update();

    /// Read the profiling clock (`CLOCK_REALTIME` with `prof_time_resolution:high`, otherwise the regular clock);
    /// there is no protection against going backwards.
    /// jemalloc: nstime_prof_update
    void profilingUpdate();

    /// jemalloc: nstime_init_update
    void initUpdate()
    {
        initZero();
        update();
    }

    /// jemalloc: nstime_prof_init_update
    void profilingInitUpdate()
    {
        initZero();
        profilingUpdate();
    }

    /// Convenience: `Nanoseconds t; t.initUpdate(); return t;`.
    static Nanoseconds now()
    {
        Nanoseconds t;
        t.initUpdate();
        return t;
    }

    static constexpr uint64_t BILLION = 1000000000ULL;
    static constexpr uint64_t MILLION = 1000000ULL;

private:
    uint64_t value;
};

static_assert(sizeof(Nanoseconds) == 8);

}
