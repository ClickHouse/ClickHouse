#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/SipHash.h>
#include <common/getThreadId.h>
#include <common/types.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_CLOCK_GETTIME;
    }
}


DB::UInt64 randomSeed()
{
    struct timespec times;
    if (clock_gettime(CLOCK_MONOTONIC, &times))
        DB::throwFromErrno("Cannot clock_gettime.", DB::ErrorCodes::CANNOT_CLOCK_GETTIME);

    /// Not cryptographically secure as time, pid and stack address can be predictable.

    SipHash hash;
    hash.update(times.tv_nsec);
    hash.update(times.tv_sec);
    hash.update(getThreadId());
    hash.update(&times);
    return hash.get64();
}
