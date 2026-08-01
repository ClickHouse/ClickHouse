#pragma once

/// Hand-written replacement for MeCab's autotools-generated `config.h`: feature flags are hardcoded
/// for ClickHouse's supported POSIX targets (Linux, macOS, FreeBSD) instead of probed by `configure`.

/* Package identification. */
#define PACKAGE "mecab"
#define PACKAGE_NAME "mecab"
#define PACKAGE_TARNAME "mecab"
#define PACKAGE_VERSION "0.996"
#define PACKAGE_STRING "mecab 0.996"
#define PACKAGE_BUGREPORT ""
#define PACKAGE_URL ""
#define VERSION "0.996"

/* Binary dictionary format version (from configure.in). */
#define DIC_VERSION 102

/* Standard C/C++ headers, present on every supported POSIX target. */
#define STDC_HEADERS 1
#define HAVE_CTYPE_H 1
#define HAVE_DIRENT_H 1
#define HAVE_DLFCN_H 1
#define HAVE_FCNTL_H 1
#define HAVE_INTTYPES_H 1
#define HAVE_MEMORY_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRINGS_H 1
#define HAVE_STRING_H 1
#define HAVE_SYS_MMAN_H 1
#define HAVE_SYS_PARAM_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_TIMES_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_UNISTD_H 1

/* Standard library functions. */
#define HAVE_GETENV 1
#define HAVE_MMAP 1
#define HAVE_OPENDIR 1
#define HAVE_UNSIGNED_LONG_LONG_INT 1

/* Threading: POSIX threads + GCC/Clang atomics + a thread-local error buffer. */
#define HAVE_PTHREAD_H 1
#define HAVE_LIBPTHREAD 1
#define HAVE_GCC_ATOMIC_OPS 1
#define HAVE_TLS_KEYWORD 1

/* UTF-8-only build: ClickHouse feeds UTF-8 and uses UTF-8 dictionaries, so `iconv` is not needed
   (`HAVE_ICONV` intentionally left undefined). */
#define MECAB_USE_UTF8_ONLY 1

/* Fallback config path; ClickHouse sets the dictionary directory programmatically, so it is unused. */
#define MECAB_DEFAULT_RC "/etc/mecabrc"

/* MeCab only needs this defined on big-endian targets. */
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define WORDS_BIGENDIAN 1
#endif
