#pragma once

/// Nanosecond time values and the allocator's clock.
/// jemalloc: `nstime.h`, `src/nstime.c`.
///
/// The clock (`nstime_get`): `CLOCK_MONOTONIC` on Linux and FreeBSD (`JEMALLOC_HAVE_CLOCK_MONOTONIC_COARSE` is not
/// defined), `gettimeofday` on Darwin (the ClickHouse fork disables `CLOCK_MONOTONIC` and `mach_absolute_time` there,
/// so that the clock matches the one used by the background thread's condition variable); `isMonotonic()` reports
/// which one. The debug-only `magic` field of `nstime_t` is not reproduced (jemalloc is built without
/// `JEMALLOC_DEBUG` in ClickHouse), so `NsTime` has the size of `nstime_t` in that configuration.

#include <allocator/Common.h>

#include <cstdint>

namespace jemalloc
{

/// Maximum supported number of seconds (~584 years) (`NSTIME_SEC_MAX`).
inline constexpr uint64_t NSTIME_SEC_MAX = 18446744072ULL;

/// jemalloc: prof_time_res_t
enum class ProfTimeRes : unsigned
{
    Default = 0,
    High = 1,
};

/// The `prof_time_resolution` option (`opt.prof_time_res`) and `prof_time_res_mode_names` are in Options.h.

/// jemalloc: nstime_t
///
/// A trivial type (zero-filled memory is a valid zero time), like the C struct.
class NsTime
{
public:
    /// jemalloc: nstime_zero
    static constexpr NsTime zero()
    {
        NsTime t;
        t.value = 0;
        return t;
    }

    static constexpr NsTime fromNs(uint64_t ns)
    {
        NsTime t;
        t.value = ns;
        return t;
    }

    /// jemalloc: nstime_init
    void init(uint64_t ns) { value = ns; }

    /// jemalloc: nstime_init2
    void init2(uint64_t sec, uint64_t nsec) { value = sec * BILLION + nsec; }

    /// jemalloc: nstime_init_zero
    void initZero() { value = 0; }

    /// jemalloc: nstime_ns
    uint64_t ns() const { return value; }

    /// jemalloc: nstime_ms
    uint64_t ms() const { return value / MILLION; }

    /// jemalloc: nstime_sec
    uint64_t sec() const { return value / BILLION; }

    /// jemalloc: nstime_nsec
    uint64_t nsec() const { return value % BILLION; }

    /// jemalloc: nstime_copy
    void copy(const NsTime & source) { value = source.value; }

    /// Returns -1, 0 or 1.
    /// jemalloc: nstime_compare
    int compare(const NsTime & other) const { return (value > other.value) - (value < other.value); }

    /// jemalloc: nstime_equals_zero
    bool equalsZero() const { return value == 0; }

    /// jemalloc: nstime_add
    void add(const NsTime & addend)
    {
        JE_ASSERT(UINT64_MAX - value >= addend.value);
        value += addend.value;
    }

    /// jemalloc: nstime_iadd
    void iadd(uint64_t addend)
    {
        JE_ASSERT(UINT64_MAX - value >= addend);
        value += addend;
    }

    /// jemalloc: nstime_subtract
    void subtract(const NsTime & subtrahend)
    {
        JE_ASSERT(compare(subtrahend) >= 0);
        value -= subtrahend.value;
    }

    /// jemalloc: nstime_isubtract
    void isubtract(uint64_t subtrahend)
    {
        JE_ASSERT(value >= subtrahend);
        value -= subtrahend;
    }

    /// jemalloc: nstime_imultiply
    void imultiply(uint64_t multiplier)
    {
        JE_ASSERT(
            (((value | multiplier) & (UINT64_MAX << (sizeof(uint64_t) << 2))) == 0) || ((value * multiplier) / multiplier == value));
        value *= multiplier;
    }

    /// jemalloc: nstime_idivide
    void idivide(uint64_t divisor)
    {
        JE_ASSERT(divisor != 0);
        value /= divisor;
    }

    /// jemalloc: nstime_divide
    uint64_t divide(const NsTime & divisor) const
    {
        JE_ASSERT(divisor.value != 0);
        return value / divisor.value;
    }

    /// jemalloc: nstime_ns_between
    static uint64_t nsBetween(const NsTime & earlier, const NsTime & later)
    {
        JE_ASSERT(later.compare(earlier) >= 0);
        return later.value - earlier.value;
    }

    /// jemalloc: nstime_ms_between
    static uint64_t msBetween(const NsTime & earlier, const NsTime & later) { return nsBetween(earlier, later) / MILLION; }

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
    void profUpdate();

    /// jemalloc: nstime_init_update
    void initUpdate()
    {
        initZero();
        update();
    }

    /// jemalloc: nstime_prof_init_update
    void profInitUpdate()
    {
        initZero();
        profUpdate();
    }

    /// Convenience: `NsTime t; t.initUpdate(); return t;`.
    static NsTime now()
    {
        NsTime t;
        t.initUpdate();
        return t;
    }

    static constexpr uint64_t BILLION = 1000000000ULL;
    static constexpr uint64_t MILLION = 1000000ULL;

private:
    uint64_t value;
};

static_assert(sizeof(NsTime) == 8);

}
