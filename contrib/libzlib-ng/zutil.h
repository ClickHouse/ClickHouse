#ifndef ZUTIL_H_
#define ZUTIL_H_
/* zutil.h -- internal interface and configuration of the compression library
 * Copyright (C) 1995-2013 Jean-loup Gailly.
 * For conditions of distribution and use, see copyright notice in zlib.h
 */

/* WARNING: this file should *not* be used by applications. It is
   part of the implementation of the compression library and is
   subject to change. Applications should only use zlib.h.
 */

/* @(#) $Id$ */

#if defined(HAVE_INTERNAL)
#  define ZLIB_INTERNAL __attribute__((visibility ("internal")))
#elif defined(HAVE_HIDDEN)
#  define ZLIB_INTERNAL __attribute__((visibility ("hidden")))
#else
#  define ZLIB_INTERNAL
#endif

#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "zlib.h"

#ifndef local
#  define local static
#endif
/* compile with -Dlocal if your debugger can't find static symbols */

typedef unsigned char uch; /* Included for compatibility with external code only */
typedef uint16_t ush;      /* Included for compatibility with external code only */
typedef unsigned long  ulg;

extern const char * const z_errmsg[10]; /* indexed by 2-zlib_error */
/* (size given to avoid silly warnings with Visual C++) */

#define ERR_MSG(err) z_errmsg[Z_NEED_DICT-(err)]

#define ERR_RETURN(strm, err) return (strm->msg = ERR_MSG(err), (err))
/* To be used only when the state is known to be valid */

        /* common constants */

#ifndef DEF_WBITS
#  define DEF_WBITS MAX_WBITS
#endif
/* default windowBits for decompression. MAX_WBITS is for compression only */

#if MAX_MEM_LEVEL >= 8
#  define DEF_MEM_LEVEL 8
#else
#  define DEF_MEM_LEVEL  MAX_MEM_LEVEL
#endif
/* default memLevel */

#define STORED_BLOCK 0
#define STATIC_TREES 1
#define DYN_TREES    2
/* The three kinds of block type */

#define MIN_MATCH  3
#define MAX_MATCH  258
/* The minimum and maximum match lengths */

#define PRESET_DICT 0x20 /* preset dictionary flag in zlib header */

        /* target dependencies */

#ifdef WIN32
#  ifndef __CYGWIN__  /* Cygwin is Unix, not Win32 */
#    define OS_CODE  0x0b
#  endif
#endif

#if (defined(_MSC_VER) && (_MSC_VER > 600))
#  define fdopen(fd, type)  _fdopen(fd, type)
#endif

/* provide prototypes for these when building zlib without LFS */
#if !defined(WIN32) && !defined(__MSYS__) && (!defined(_LARGEFILE64_SOURCE) || _LFS64_LARGEFILE-0 == 0)
    ZEXTERN uint32_t ZEXPORT adler32_combine64(uint32_t, uint32_t, z_off_t);
    ZEXTERN uint32_t ZEXPORT crc32_combine64(uint32_t, uint32_t, z_off_t);
#endif

/* MS Visual Studio does not allow inline in C, only C++.
   But it provides __inline instead, so use that. */
#if defined(_MSC_VER) && !defined(inline)
#  define inline __inline
#endif

        /* common defaults */

#ifndef OS_CODE
#  define OS_CODE  0x03  /* assume Unix */
#endif

#ifndef F_OPEN
#  define F_OPEN(name, mode) fopen((name), (mode))
#endif

         /* functions */

/* Diagnostic functions */
#ifdef DEBUG
#   include <stdio.h>
    extern int ZLIB_INTERNAL z_verbose;
    extern void ZLIB_INTERNAL z_error(char *m);
#   define Assert(cond, msg) {if(!(cond)) z_error(msg);}
#   define Trace(x) {if (z_verbose >= 0) fprintf x;}
#   define Tracev(x) {if (z_verbose > 0) fprintf x;}
#   define Tracevv(x) {if (z_verbose > 1) fprintf x;}
#   define Tracec(c, x) {if (z_verbose > 0 && (c)) fprintf x;}
#   define Tracecv(c, x) {if (z_verbose > 1 && (c)) fprintf x;}
#else
#   define Assert(cond, msg)
#   define Trace(x)
#   define Tracev(x)
#   define Tracevv(x)
#   define Tracec(c, x)
#   define Tracecv(c, x)
#endif

void ZLIB_INTERNAL *zcalloc(void *opaque, unsigned items, unsigned size);
void ZLIB_INTERNAL   zcfree(void *opaque, void *ptr);

#define ZALLOC(strm, items, size) (*((strm)->zalloc))((strm)->opaque, (items), (size))
#define ZFREE(strm, addr)         (*((strm)->zfree))((strm)->opaque, (void *)(addr))
#define TRY_FREE(s, p) {if (p) ZFREE(s, p);}

/* Reverse the bytes in a 32-bit value. Use compiler intrinsics when
   possible to take advantage of hardware implementations. */
#if defined(WIN32) && (_MSC_VER >= 1300)
#  pragma intrinsic(_byteswap_ulong)
#  define ZSWAP32(q) _byteswap_ulong(q)

#elif defined(__Clang__) || (defined(__GNUC__) && \
        (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2)))
#  define ZSWAP32(q) __builtin_bswap32(q)

#elif defined(__GNUC__) && (__GNUC__ >= 2) && defined(__linux__)
#  include <byteswap.h>
#  define ZSWAP32(q) bswap_32(q)

#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || defined(__DragonFly__)
#  include <sys/endian.h>
#  define ZSWAP32(q) bswap32(q)

#elif defined(__INTEL_COMPILER)
#  define ZSWAP32(q) _bswap(q)

#else
#  define ZSWAP32(q) ((((q) >> 24) & 0xff) + (((q) >> 8) & 0xff00) + \
                    (((q) & 0xff00) << 8) + (((q) & 0xff) << 24))
#endif /* ZSWAP32 */

/* Only enable likely/unlikely if the compiler is known to support it */
#if (defined(__GNUC__) && (__GNUC__ >= 3)) || defined(__INTEL_COMPILER) || defined(__Clang__)
#  ifndef likely
#    define likely(x)      __builtin_expect(!!(x), 1)
#  endif
#  ifndef unlikely
#    define unlikely(x)    __builtin_expect(!!(x), 0)
#  endif
#else
#  ifndef likely
#    define likely(x)      x
#  endif
#  ifndef unlikely
#    define unlikely(x)    x
#  endif
#endif /* (un)likely */

#if defined(_MSC_VER)
#define ALIGNED_(x) __declspec(align(x))
#else
#if defined(__GNUC__)
#define ALIGNED_(x) __attribute__ ((aligned(x)))
#endif
#endif

#endif /* ZUTIL_H_ */
