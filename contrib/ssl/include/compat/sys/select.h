/*
 * Public domain
 * sys/select.h compatibility shim
 */

#ifndef _WIN32
#include_next <sys/select.h>
#else
#include <win32netcompat.h>
#endif
