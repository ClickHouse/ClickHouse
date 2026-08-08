#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/SipHash.h>
#include <base/getThreadId.h>
#include <base/types.h>

#if defined(__linux__)
#include <sys/utsname.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CLOCK_GETTIME;
}
}


UInt64 randomSeed()
{
    struct timespec times;
    if (clock_gettime(CLOCK_MONOTONIC, &times))
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    /// Not cryptographically secure as time, pid and stack address can be predictable.

    SipHash hash;
    hash.update(times.tv_nsec);
    hash.update(times.tv_sec);
    hash.update(getThreadId());

    /// It makes sense to add something like hostname to avoid seed collision when multiple servers start simultaneously.
    /// But randomSeed() must be signal-safe and gethostname and similar functions are not.
    /// Let's try to get utsname.nodename using uname syscall (it's signal-safe).
#if defined(__linux__)
    struct utsname sysinfo;
    if (uname(&sysinfo) == 0)
        hash.update<std::identity>(sysinfo);
#endif

    return hash.get64();
}
