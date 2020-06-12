/*
 * Public domain
 * sys/time.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_SYS_TIME_H
#define LIBCRYPTOCOMPAT_SYS_TIME_H

#ifdef _MSC_VER
#include <winsock2.h>
int gettimeofday(struct timeval *tp, void *tzp);
#else
#include_next <sys/time.h>
#endif

#endif
