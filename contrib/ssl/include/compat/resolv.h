/*
 * Public domain
 * resolv.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_RESOLV_H
#define LIBCRYPTOCOMPAT_RESOLV_H

#ifdef _MSC_VER
#if _MSC_VER >= 1900
#include <../ucrt/resolv.h>
#else
#include <../include/resolv.h>
#endif
#else
#include_next <resolv.h>
#endif

#ifndef HAVE_B64_NTOP
int b64_ntop(unsigned char const *, size_t, char *, size_t);
int b64_pton(char const *, unsigned char *, size_t);
#endif

#endif
