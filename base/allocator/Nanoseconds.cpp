#include <allocator/Nanoseconds.h>

#include <allocator/Options.h>

#include <time.h>
#include <sys/time.h>

namespace jemalloc
{

namespace
{

/// jemalloc: nstime_get
void getTime(Nanoseconds & time)
{
    if constexpr (config::have_clock_monotonic)
    {
        struct timespec time_spec;
        clock_gettime(CLOCK_MONOTONIC, &time_spec);
        time.initSecondsAndNanoseconds(uint64_t(time_spec.tv_sec), uint64_t(time_spec.tv_nsec));
    }
    else
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        time.initSecondsAndNanoseconds(uint64_t(tv.tv_sec), uint64_t(tv.tv_usec) * 1000);
    }
}

/// jemalloc: nstime_get_realtime
void getRealtime(Nanoseconds & time)
{
    struct timespec time_spec;
    clock_gettime(CLOCK_REALTIME, &time_spec);
    time.initSecondsAndNanoseconds(uint64_t(time_spec.tv_sec), uint64_t(time_spec.tv_nsec));
}

}

/// jemalloc: nstime_ns_since
uint64_t Nanoseconds::nsSince() const
{
    Nanoseconds now = *this;
    now.update();
    return nsBetween(*this, now);
}

/// jemalloc: nstime_update_impl
void Nanoseconds::update()
{
    Nanoseconds old_time = *this;
    getTime(*this);

    /// Handle non-monotonic clocks.
    if (ALLOCATOR_UNLIKELY(old_time.compare(*this) > 0))
        *this = old_time;
}

/// jemalloc: nstime_prof_update_impl
void Nanoseconds::profilingUpdate()
{
    if (options.profiling_time_resolution == ProfilingTimeResolution::High)
        getRealtime(*this);
    else
        getTime(*this);
}

}
