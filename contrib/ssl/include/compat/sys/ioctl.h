/*
 * Public domain
 * sys/ioctl.h compatibility shim
 */

#ifndef _WIN32
#include_next <sys/ioctl.h>
#else
#include <win32netcompat.h>
#define ioctl(fd, type, arg) ioctlsocket(fd, type, arg)
#endif
