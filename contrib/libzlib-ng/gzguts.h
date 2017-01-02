#ifndef GZGUTS_H_
#define GZGUTS_H_
/* gzguts.h -- zlib internal header definitions for gz* operations
 * Copyright (C) 2004, 2005, 2010, 2011, 2012, 2013 Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
 */

#ifdef _LARGEFILE64_SOURCE
#  ifndef _LARGEFILE_SOURCE
#    define _LARGEFILE_SOURCE 1
#  endif
#  ifdef _FILE_OFFSET_BITS
#    undef _FILE_OFFSET_BITS
#  endif
#endif

#if defined(HAVE_INTERNAL)
#  define ZLIB_INTERNAL __attribute__((visibility ("internal")))
#elif defined(HAVE_HIDDEN)
#  define ZLIB_INTERNAL __attribute__((visibility ("hidden")))
#else
#  define ZLIB_INTERNAL
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <fcntl.h>
#include "zlib.h"

#ifdef WIN32
#  include <stddef.h>
#endif

#if defined(_MSC_VER) || defined(WIN32)
#  include <io.h>
#endif

#if defined(_WIN32) || defined(__CYGWIN__) || defined(__MINGW__)
#  define WIDECHAR
#endif

#ifdef WINAPI_FAMILY
#  define open _open
#  define read _read
#  define write _write
#  define close _close
#endif

/* In Win32, vsnprintf is available as the "non-ANSI" _vsnprintf. */
#if !defined(STDC99) && !defined(__CYGWIN__) && !defined(__MINGW__) && defined(WIN32)
#  if !defined(vsnprintf)
#    if !defined(_MSC_VER) || ( defined(_MSC_VER) && _MSC_VER < 1500 )
#       define vsnprintf _vsnprintf
#    endif
#  endif
#endif

/* unlike snprintf (which is required in C99), _snprintf does not guarantee
   null termination of the result -- however this is only used in gzlib.c
   where the result is assured to fit in the space provided */
#if defined(_MSC_VER) && _MSC_VER < 1900
#  define snprintf _snprintf
#endif

#ifndef local
#  define local static
#endif
/* compile with -Dlocal if your debugger can't find static symbols */

/* get errno and strerror definition */
#ifndef NO_STRERROR
#  include <errno.h>
#  define zstrerror() strerror(errno)
#else
#  define zstrerror() "stdio error (consult errno)"
#endif

/* provide prototypes for these when building zlib without LFS */
#if (!defined(_LARGEFILE64_SOURCE) || _LFS64_LARGEFILE-0 == 0) && defined(WITH_GZFILEOP)
    ZEXTERN gzFile ZEXPORT gzopen64(const char *, const char *);
    ZEXTERN z_off64_t ZEXPORT gzseek64(gzFile, z_off64_t, int);
    ZEXTERN z_off64_t ZEXPORT gztell64(gzFile);
    ZEXTERN z_off64_t ZEXPORT gzoffset64(gzFile);
#endif

/* default memLevel */
#if MAX_MEM_LEVEL >= 8
#  define DEF_MEM_LEVEL 8
#else
#  define DEF_MEM_LEVEL  MAX_MEM_LEVEL
#endif

/* default i/o buffer size -- double this for output when reading (this and
   twice this must be able to fit in an unsigned type) */
#define GZBUFSIZE 8192

/* gzip modes, also provide a little integrity check on the passed structure */
#define GZ_NONE 0
#define GZ_READ 7247
#define GZ_WRITE 31153
#define GZ_APPEND 1     /* mode set to GZ_WRITE after the file is opened */

/* values for gz_state how */
#define LOOK 0      /* look for a gzip header */
#define COPY 1      /* copy input directly */
#define GZIP 2      /* decompress a gzip stream */

/* internal gzip file state data structure */
typedef struct {
        /* exposed contents for gzgetc() macro */
    struct gzFile_s x;      /* "x" for exposed */
                            /* x.have: number of bytes available at x.next */
                            /* x.next: next output data to deliver or write */
                            /* x.pos: current position in uncompressed data */
        /* used for both reading and writing */
    int mode;               /* see gzip modes above */
    int fd;                 /* file descriptor */
    char *path;             /* path or fd for error messages */
    unsigned size;          /* buffer size, zero if not allocated yet */
    unsigned want;          /* requested buffer size, default is GZBUFSIZE */
    unsigned char *in;      /* input buffer (double-sized when writing) */
    unsigned char *out;     /* output buffer (double-sized when reading) */
    int direct;             /* 0 if processing gzip, 1 if transparent */
        /* just for reading */
    int how;                /* 0: get header, 1: copy, 2: decompress */
    z_off64_t start;        /* where the gzip data started, for rewinding */
    int eof;                /* true if end of input file reached */
    int past;               /* true if read requested past end */
        /* just for writing */
    int level;              /* compression level */
    int strategy;           /* compression strategy */
        /* seek request */
    z_off64_t skip;         /* amount to skip (already rewound if backwards) */
    int seek;               /* true if seek request pending */
        /* error information */
    int err;                /* error code */
    char *msg;              /* error message */
        /* zlib inflate or deflate stream */
    z_stream strm;          /* stream structure in-place (not a pointer) */
} gz_state;
typedef gz_state *gz_statep;

/* shared functions */
void ZLIB_INTERNAL gz_error(gz_statep, int, const char *);

/* GT_OFF(x), where x is an unsigned value, is true if x > maximum z_off64_t
   value -- needed when comparing unsigned to z_off64_t, which is signed
   (possible z_off64_t types off_t, off64_t, and long are all signed) */
#ifdef INT_MAX
#  define GT_OFF(x) (sizeof(int) == sizeof(z_off64_t) && (x) > INT_MAX)
#else
unsigned ZLIB_INTERNAL gz_intmax(void);
#  define GT_OFF(x) (sizeof(int) == sizeof(z_off64_t) && (x) > gz_intmax())
#endif

#endif /* GZGUTS_H_ */
