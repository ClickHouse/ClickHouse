#include <time.h>
#include <unistd.h>
#include <sys/types.h>

#include <DB/Common/Exception.h>
#include <DB/Common/randomSeed.h>


namespace DB
{
	namespace ErrorCodes
	{
		extern const int CANNOT_CLOCK_GETTIME;
	}
}


uint64_t randomSeed()
{
	struct timespec times;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
		DB::throwFromErrno("Cannot clock_gettime.", DB::ErrorCodes::CANNOT_CLOCK_GETTIME);
	return times.tv_nsec + times.tv_sec + getpid();
}
