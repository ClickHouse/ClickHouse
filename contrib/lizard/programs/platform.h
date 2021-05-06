/*
    platform.h - compiler and OS detection
    Copyright (C) 2016-present, Przemyslaw Skibinski, Yann Collet

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

#ifndef PLATFORM_H_MODULE
#define PLATFORM_H_MODULE

#if defined (__cplusplus)
extern "C" {
#endif



/* **************************************
*  Compiler Options
****************************************/
#if defined(_MSC_VER)
#  define _CRT_SECURE_NO_WARNINGS   /* Disable Visual Studio warning messages for fopen, strncpy, strerror */
#  define _CRT_SECURE_NO_DEPRECATE  /* VS2005 - must be declared before <io.h> and <windows.h> */ 
#  if (_MSC_VER <= 1800)            /* (1800 = Visual Studio 2013) */
#    define snprintf sprintf_s      /* snprintf unsupported by Visual <= 2013 */
#  endif
#endif


/* **************************************
*  Detect 64-bit OS
*  http://nadeausoftware.com/articles/2012/02/c_c_tip_how_detect_processor_type_using_compiler_predefined_macros
****************************************/
#if defined __ia64 || defined _M_IA64                                                                               /* Intel Itanium */ \
  || defined __powerpc64__ || defined __ppc64__ || defined __PPC64__                                                /* POWER 64-bit */  \
  || (defined __sparc && (defined __sparcv9 || defined __sparc_v9__ || defined __arch64__)) || defined __sparc64__  /* SPARC 64-bit */  \
  || defined __x86_64__s || defined _M_X64                                                                          /* x86 64-bit */    \
  || defined __arm64__ || defined __aarch64__ || defined __ARM64_ARCH_8__                                           /* ARM 64-bit */    \
  || (defined __mips  && (__mips == 64 || __mips == 4 || __mips == 3))                                              /* MIPS 64-bit */   \
  || defined _LP64 || defined __LP64__ /* NetBSD, OpenBSD */ || defined __64BIT__ /* AIX */ || defined _ADDR64 /* Cray */               \
  || (defined __SIZEOF_POINTER__ && __SIZEOF_POINTER__ == 8) /* gcc */
#  if !defined(__64BIT__)
#    define __64BIT__  1
#  endif
#endif


/* *********************************************************
*  Turn on Large Files support (>4GB) for 32-bit Linux/Unix
***********************************************************/
#if !defined(__64BIT__) || defined(__MINGW32__)       /* No point defining Large file for 64 bit but MinGW-w64 requires it */
#  if !defined(_FILE_OFFSET_BITS)   
#    define _FILE_OFFSET_BITS 64                      /* turn off_t into a 64-bit type for ftello, fseeko */
#  endif
#  if !defined(_LARGEFILE_SOURCE)                     /* obsolete macro, replaced with _FILE_OFFSET_BITS */
#    define _LARGEFILE_SOURCE 1                       /* Large File Support extension (LFS) - fseeko, ftello */
#  endif
#  if defined(_AIX) || defined(__hpux)
#    define _LARGE_FILES                              /* Large file support on 32-bits AIX and HP-UX */
#  endif
#endif


/* ************************************************************
*  Detect POSIX version
*  PLATFORM_POSIX_VERSION = -1 for non-Unix e.g. Windows
*  PLATFORM_POSIX_VERSION = 0 for Unix-like non-POSIX
*  PLATFORM_POSIX_VERSION >= 1 is equal to found _POSIX_VERSION
***************************************************************/
#if !defined(_WIN32) && (defined(__unix__) || defined(__unix) || (defined(__APPLE__) && defined(__MACH__)) /* UNIX-like OS */ \
   || defined(__midipix__) || defined(__VMS))
#  if (defined(__APPLE__) && defined(__MACH__)) || defined(__SVR4) || defined(_AIX) || defined(__hpux) /* POSIX.1–2001 (SUSv3) conformant */ \
     || defined(__DragonFly__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)  /* BSD distros */
#    define PLATFORM_POSIX_VERSION 200112L
#  else
#    if defined(__linux__) || defined(__linux)
#      ifndef _POSIX_C_SOURCE
#        define _POSIX_C_SOURCE 200112L  /* use feature test macro */
#      endif
#    endif
#    include <unistd.h>  /* declares _POSIX_VERSION */
#    if defined(_POSIX_VERSION)  /* POSIX compliant */
#      define PLATFORM_POSIX_VERSION _POSIX_VERSION
#    else
#      define PLATFORM_POSIX_VERSION 0
#    endif
#  endif
#endif
#if !defined(PLATFORM_POSIX_VERSION)
#  define PLATFORM_POSIX_VERSION -1
#endif


/*-*********************************************
*  Detect if isatty() and fileno() are available
************************************************/
#if (defined(__linux__) && (PLATFORM_POSIX_VERSION >= 1)) || (PLATFORM_POSIX_VERSION >= 200112L) || defined(__DJGPP__)
#  include <unistd.h>   /* isatty */
#  define IS_CONSOLE(stdStream) isatty(fileno(stdStream))
#elif defined(MSDOS) || defined(OS2) || defined(WIN32) || defined(_WIN32) || defined(__CYGWIN__)
#  include <io.h>       /* _isatty */
#  define IS_CONSOLE(stdStream) _isatty(_fileno(stdStream))
#else
#  define IS_CONSOLE(stdStream) 0
#endif


/******************************
*  OS-specific Includes
******************************/
#if defined(MSDOS) || defined(OS2) || defined(WIN32) || defined(_WIN32)
#  include <fcntl.h>   /* _O_BINARY */
#  include <io.h>      /* _setmode, _fileno, _get_osfhandle */
#  if !defined(__DJGPP__)
#    include <windows.h> /* DeviceIoControl, HANDLE, FSCTL_SET_SPARSE */
#    include <winioctl.h> /* FSCTL_SET_SPARSE */
#    define SET_BINARY_MODE(file) { int unused=_setmode(_fileno(file), _O_BINARY); (void)unused; }
#    define SET_SPARSE_FILE_MODE(file) { DWORD dw; DeviceIoControl((HANDLE) _get_osfhandle(_fileno(file)), FSCTL_SET_SPARSE, 0, 0, 0, 0, &dw, 0); }
#  else
#    define SET_BINARY_MODE(file) setmode(fileno(file), O_BINARY)
#    define SET_SPARSE_FILE_MODE(file)
#  endif
#else
#  define SET_BINARY_MODE(file)
#  define SET_SPARSE_FILE_MODE(file)
#endif



#if defined (__cplusplus)
}
#endif

#endif /* PLATFORM_H_MODULE */
