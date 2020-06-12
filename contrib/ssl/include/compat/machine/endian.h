/*
 * Public domain
 * machine/endian.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_BYTE_ORDER_H_
#define LIBCRYPTOCOMPAT_BYTE_ORDER_H_

#if defined(_WIN32)

#define LITTLE_ENDIAN  1234
#define BIG_ENDIAN 4321
#define PDP_ENDIAN	3412

/*
 * Use GCC and Visual Studio compiler defines to determine endian.
 */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define BYTE_ORDER LITTLE_ENDIAN
#else
#define BYTE_ORDER BIG_ENDIAN
#endif

#elif defined(__linux__)
#include <endian.h>

#elif defined(__sun) || defined(_AIX) || defined(__hpux)
#include <sys/types.h>
#include <arpa/nameser_compat.h>

#elif defined(__sgi)
#include <standards.h>
#include <sys/endian.h>

#else
#include_next <machine/endian.h>

#endif

#endif
