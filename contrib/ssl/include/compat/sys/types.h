/*
 * Public domain
 * sys/types.h compatibility shim
 */

#ifdef _MSC_VER
#if _MSC_VER >= 1900
#include <../ucrt/sys/types.h>
#else
#include <../include/sys/types.h>
#endif
#else
#include_next <sys/types.h>
#endif

#ifndef LIBCRYPTOCOMPAT_SYS_TYPES_H
#define LIBCRYPTOCOMPAT_SYS_TYPES_H

#include <stdint.h>

#ifdef __MINGW32__
#include <_bsd_types.h>
typedef uint32_t        in_addr_t;
#endif

#ifdef _MSC_VER
typedef unsigned char   u_char;
typedef unsigned short  u_short;
typedef unsigned int    u_int;
typedef uint32_t        in_addr_t;

#include <basetsd.h>
typedef SSIZE_T ssize_t;

#ifndef SSIZE_MAX
#ifdef _WIN64
#define SSIZE_MAX _I64_MAX
#else
#define SSIZE_MAX INT_MAX
#endif
#endif

#endif

#if !defined(HAVE_ATTRIBUTE__BOUNDED__) && !defined(__bounded__)
# define __bounded__(x, y, z)
#endif

#ifdef _WIN32
#define __warn_references(sym,msg)
#else

#ifndef __warn_references

#ifndef __STRING
#define __STRING(x) #x
#endif

#if defined(__GNUC__)  && defined (HAS_GNU_WARNING_LONG)
#define __warn_references(sym,msg)          \
  __asm__(".section .gnu.warning." __STRING(sym)  \
         " ; .ascii \"" msg "\" ; .text");
#else
#define __warn_references(sym,msg)
#endif

#endif /* __warn_references */
#endif /* _WIN32 */

#endif
