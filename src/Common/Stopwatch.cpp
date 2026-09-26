#include <Common/Stopwatch.h>

#include <base/defines.h>

#include <cerrno>
#include <system_error>


/// Out of line and uninstrumented: `clock_gettime` needs a `timespec` address, and inlining that
/// buffer would put a `-fstack-protector-strong` canary on every caller.
NO_STACK_PROTECTOR NO_INLINE UInt64 clock_gettime_ns(clockid_t clock_type)
{
    struct timespec ts{};
    if (0 != clock_gettime(clock_type, &ts))
        throw std::system_error(std::error_code(errno, std::system_category()));
    return UInt64(ts.tv_sec * 1000000000LL + ts.tv_nsec);
}
