#include <time.h>
#include <unistd.h>
#include <sys/types.h>

#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Core/Types.h>

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif


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
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
        DB::throwFromErrno("Cannot clock_gettime.", DB::ErrorCodes::CANNOT_CLOCK_GETTIME);
    return times.tv_nsec + times.tv_sec + getpid();
}
