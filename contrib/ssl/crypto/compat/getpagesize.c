/* $OpenBSD$ */

#include <unistd.h>

#ifdef _MSC_VER
#include <windows.h>
#endif

int
getpagesize(void) {
#ifdef _MSC_VER
	SYSTEM_INFO system_info;
	GetSystemInfo(&system_info);
	return system_info.dwPageSize;
#else
	return sysconf(_SC_PAGESIZE);
#endif
}
