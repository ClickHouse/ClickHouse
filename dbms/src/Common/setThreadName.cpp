#include <sys/prctl.h>
#include <DB/Common/Exception.h>
#include <DB/Common/setThreadName.h>


void setThreadName(const char * name)
{
	if (0 != prctl(PR_SET_NAME, name, 0, 0, 0))
		DB::throwFromErrno("Cannot set thread name with prctl(PR_SET_NAME...)");
}
