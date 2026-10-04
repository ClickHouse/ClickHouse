#include <allocator/NsTime.h>

#include <allocator/Options.h>

#include <sys/time.h>
#include <time.h>

namespace jemalloc
{

namespace
{

/// jemalloc: nstime_get
void getTime(NsTime & time)
{
    if constexpr (config::have_clock_monotonic)
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        time.init2(uint64_t(ts.tv_sec), uint64_t(ts.tv_nsec));
    }
    else
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        time.init2(uint64_t(tv.tv_sec), uint64_t(tv.tv_usec) * 1000);
    }
}

/// jemalloc: nstime_get_realtime
void getRealtime(NsTime & time)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time.init2(uint64_t(ts.tv_sec), uint64_t(ts.tv_nsec));
}

}

/// jemalloc: nstime_ns_since
uint64_t NsTime::nsSince() const
{
    NsTime now = *this;
    now.update();
    return nsBetween(*this, now);
}

/// jemalloc: nstime_update_impl
void NsTime::update()
{
    NsTime old_time = *this;
    getTime(*this);

    /// Handle non-monotonic clocks.
    if (JE_UNLIKELY(old_time.compare(*this) > 0))
        *this = old_time;
}

/// jemalloc: nstime_prof_update_impl
void NsTime::profUpdate()
{
    if (opt.prof_time_res == ProfTimeRes::High)
        getRealtime(*this);
    else
        getTime(*this);
}

}
