/**
 *  Endian.h
 *
 *  On Apple systems, there are no macro's to convert between little
 *  and big endian byte orders. This header file adds the missing macros
 *
 *  @author madmongo1 <https://github.com/madmongo1>
 *
 *  And we have also copied code from the "portable_endian.h" file by
 *  Mathias Panzenböck. His license:
 *
 *  "License": Public Domain
 *      I, Mathias Panzenböck, place this file hereby into the public
 *      domain. Use it at your own risk for whatever you like. In case
 *      there are jurisdictions that don't support putting things in the
 *      public domain you can also consider it to be "dual licensed"
 *      under the BSD, MIT and Apache licenses, if you want to. This
 *      code is trivial anyway. Consider it an example on how to get the
 *      endian conversion functions on different platforms.
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  The contents of the file are only relevant for Apple
 */
#if defined(__APPLE__)

// dependencies
#include <machine/endian.h>
#include <libkern/OSByteOrder.h>

// define 16 bit macros
#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

// define 32 bit macros
#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

// define 64 but macros
#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

/**
 *  And on Windows systems weird things are going on as well
 */
#elif (defined(_WIN16) || defined(_WIN32) || defined(_WIN64) || defined(__WINDOWS__)) && !defined(__CYGWIN__)

#include <winsock2.h>
#pragma comment(lib,"Ws2_32.lib")
//# include <sys/param.h>

#if BYTE_ORDER == LITTLE_ENDIAN

#define htobe16(x) htons(x)
#define htole16(x) (x)
#define be16toh(x) ntohs(x)
#define le16toh(x) (x)

#define htobe32(x) htonl(x)
#define htole32(x) (x)
#define be32toh(x) ntohl(x)
#define le32toh(x) (x)

#define htobe64(x) ((1==htonl(1)) ? (x) : ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#define htole64(x) (x)
#define be64toh(x) ((1==ntohl(1)) ? (x) : ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#define le64toh(x) (x)

#elif BYTE_ORDER == BIG_ENDIAN

/* that would be xbox 360 */
#define htobe16(x) (x)
#define htole16(x) __builtin_bswap16(x)
#define be16toh(x) (x)
#define le16toh(x) __builtin_bswap16(x)

#define htobe32(x) (x)
#define htole32(x) __builtin_bswap32(x)
#define be32toh(x) (x)
#define le32toh(x) __builtin_bswap32(x)

#define htobe64(x) (x)
#define htole64(x) __builtin_bswap64(x)
#define be64toh(x) (x)
#define le64toh(x) __builtin_bswap64(x)

#else

#error byte order not supported

#endif

#define __BYTE_ORDER    BYTE_ORDER
#define __BIG_ENDIAN    BIG_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __PDP_ENDIAN    PDP_ENDIAN

/**
 *  OpenBSD handling
 */
#elif defined(__OpenBSD__)

#include <sys/endian.h>

/**
 *  NetBSD handling
 */
#elif defined(__NetBSD__) || defined(__DragonFly__)

#include <sys/endian.h>
#define be16toh(x) betoh16(x)
#define le16toh(x) letoh16(x)
#define be32toh(x) betoh32(x)
#define le32toh(x) letoh32(x)
#define be64toh(x) betoh64(x)
#define le64toh(x) letoh64(x)

/**
 * FreeBSD handling
 */
#elif defined(__FreeBSD__)

#include <sys/endian.h>

/**
 *  Not on apple, and not on windows
 */
#else

// this is the normal linux way of doing things
#include <endian.h>

// end of "#if defined(__APPLE__)"
#endif

