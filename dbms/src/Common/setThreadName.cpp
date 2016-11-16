#ifndef __APPLE__
#include <sys/prctl.h>
#else
#include <pthread.h>
#endif
#include <DB/Common/Exception.h>
#include <DB/Common/setThreadName.h>


void setThreadName(const char * name)
{
#ifndef __APPLE__
	if (0 != prctl(PR_SET_NAME, name, 0, 0, 0))
#else
	if (0 != pthread_setname_np(name))
#endif
		DB::throwFromErrno("Cannot set thread name with prctl(PR_SET_NAME...)");
}
