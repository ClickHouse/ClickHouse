/*
 * Public domain
 * sys/param.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_SYS_PARAM_H
#define LIBCRYPTOCOMPAT_SYS_PARAM_H

#ifdef _MSC_VER
#include <winsock2.h>
#else
#include_next <sys/param.h>
#endif

#endif
