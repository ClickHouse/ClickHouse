/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02111-1301, USA */

/* This is the main include file that should included 'first' in every
   C file. */

#ifndef _global_h
#define _global_h

#ifdef _WIN32
#include <winsock2.h>
#include <Windows.h>
#include <stdlib.h>
#define strcasecmp _stricmp
#define sleep(x) Sleep(1000*(x))
#ifdef _MSC_VER
#define inline __inline
#if _MSC_VER < 1900
#define snprintf _snprintf
#endif
#endif
#define STDCALL __stdcall 
#endif

#include <ma_config.h>
#include <assert.h>
#ifndef __GNUC__
#define  __attribute(A)
#endif

/* Fix problem with S_ISLNK() on Linux */
#if defined(HAVE_LINUXTHREADS)
#undef  _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

/* The client defines this to avoid all thread code */
#if defined(UNDEF_THREADS_HACK)
#undef THREAD
#undef HAVE_mit_thread
#undef HAVE_LINUXTHREADS
#undef HAVE_UNIXWARE7_THREADS
#endif

#ifdef HAVE_THREADS_WITHOUT_SOCKETS
/* MIT pthreads does not work with unix sockets */
#undef HAVE_SYS_UN_H
#endif

#define __EXTENSIONS__ 1	/* We want some extension */
#ifndef __STDC_EXT__
#define __STDC_EXT__ 1          /* To get large file support on hpux */
#endif

#if defined(THREAD) && !defined(_WIN32)
#ifndef _POSIX_PTHREAD_SEMANTICS
#define _POSIX_PTHREAD_SEMANTICS /* We want posix threads */
#endif
/* was #if defined(HAVE_LINUXTHREADS) || defined(HAVE_DEC_THREADS) || defined(HPUX) */
#if !defined(SCO)
#define _REENTRANT	1	/* Some thread libraries require this */
#endif
#if !defined(_THREAD_SAFE) && !defined(_AIX)
#define _THREAD_SAFE            /* Required for OSF1 */
#endif
#ifndef HAVE_mit_thread
#ifdef HAVE_UNIXWARE7_THREADS
#include <thread.h>
#else
#include <pthread.h>		/* AIX must have this included first */
#endif /* HAVE_UNIXWARE7_THREADS */
#endif /* HAVE_mit_thread */
#if !defined(SCO) && !defined(_REENTRANT)
#define _REENTRANT	1	/* Threads requires reentrant code */
#endif
#endif /* THREAD */

/* Go around some bugs in different OS and compilers */
#ifdef _AIX			/* By soren@t.dk */
#define _H_STRINGS
#define _SYS_STREAM_H
#define _AIX32_CURSES
#define ulonglong2double(A) my_ulonglong2double(A)
#define my_off_t2double(A)  my_ulonglong2double(A)
#ifdef	__cplusplus
extern "C" {
#endif
double my_ulonglong2double(unsigned long long A);
#ifdef	__cplusplus
}
#endif
#endif /* _AIX */

#ifdef HAVE_BROKEN_SNPRINTF	/* HPUX 10.20 don't have this defined */
#undef HAVE_SNPRINTF
#endif
#ifdef HAVE_BROKEN_PREAD	/* These doesn't work on HPUX 11.x */
#undef HAVE_PREAD
#undef HAVE_PWRITE
#endif
#if defined(HAVE_BROKEN_INLINE) && !defined(__cplusplus)
#undef inline
#define inline
#endif

#ifdef UNDEF_HAVE_GETHOSTBYNAME_R		/* For OSF4.x */
#undef HAVE_GETHOSTBYNAME_R
#endif
#ifdef UNDEF_HAVE_INITGROUPS			/* For AIX 4.3 */
#undef HAVE_INITGROUPS
#endif

/* Fix a bug in gcc 2.8.0 on IRIX 6.2 */
#if SIZEOF_LONG == 4 && defined(__LONG_MAX__)
#undef __LONG_MAX__             /* Is a longlong value in gcc 2.8.0 ??? */
#define __LONG_MAX__ 2147483647
#endif

/* Fix problem when linking c++ programs with gcc 3.x */
#ifdef DEFINE_CXA_PURE_VIRTUAL
#define FIX_GCC_LINKING_PROBLEM extern "C" { int __cxa_pure_virtual() {return 0;} }
#else
#define FIX_GCC_LINKING_PROBLEM
#endif

/* egcs 1.1.2 has a problem with memcpy on Alpha */
#if defined(__GNUC__) && defined(__alpha__) && ! (__GNUC__ > 2 || (__GNUC__ == 2 &&  __GNUC_MINOR__ >= 95))
#define BAD_MEMCPY
#endif

/* In Linux-alpha we have atomic.h if we are using gcc */
#if defined(HAVE_LINUXTHREADS) && defined(__GNUC__) && defined(__alpha__) && (__GNUC__ > 2 || ( __GNUC__ == 2 &&  __GNUC_MINOR__ >= 95)) && !defined(HAVE_ATOMIC_ADD)
#define HAVE_ATOMIC_ADD
#define HAVE_ATOMIC_SUB
#endif

/* In Linux-ia64 including atomic.h will give us an error */
#if (defined(HAVE_LINUXTHREADS) && defined(__GNUC__) && (defined(__ia64__) || defined(__powerpc64__))) || !defined(THREAD)
#undef HAVE_ATOMIC_ADD
#undef HAVE_ATOMIC_SUB
#endif

#if defined(_lint) && !defined(lint)
#define lint
#endif
#if SIZEOF_LONG_LONG > 4 && !defined(_LONG_LONG)
#define _LONG_LONG 1		/* For AIX string library */
#endif

#ifndef stdin
#include <stdio.h>
#endif
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_STDDEF_H
#include <stddef.h>
#endif

#include <math.h>
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif
#ifdef HAVE_FLOAT_H
#include <float.h>
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif /* TIME_WITH_SYS_TIME */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#if defined(__cplusplus) && defined(NO_CPLUSPLUS_ALLOCA)
#undef HAVE_ALLOCA
#undef HAVE_ALLOCA_H
#endif
#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif
#ifdef HAVE_ATOMIC_ADD
#define __SMP__
#define CONFIG_SMP
#include <asm/atomic.h>
#endif
#include <errno.h>				/* Recommended by debian */
#include <assert.h>

/* Go around some bugs in different OS and compilers */
#if defined(_HPUX_SOURCE) && defined(HAVE_SYS_STREAM_H)
#include <sys/stream.h>		/* HPUX 10.20 defines ulong here. UGLY !!! */
#define HAVE_ULONG
#endif
#ifdef DONT_USE_FINITE		/* HPUX 11.x has is_finite() */
#undef HAVE_FINITE
#endif
#if defined(HPUX) && defined(_LARGEFILE64_SOURCE) && defined(THREAD)
/* Fix bug in setrlimit */
#undef setrlimit
#define setrlimit cma_setrlimit64
#endif

/* We can not live without these */

#define USE_MYFUNC 1		/* Must use syscall indirection */
#define MASTER 1		/* Compile without unireg */
#define ENGLISH 1		/* Messages in English */
#define POSIX_MISTAKE 1		/* regexp: Fix stupid spec error */
#define USE_REGEX 1		/* We want the use the regex library */
/* Do not define for ultra sparcs */
#define USE_BMOVE512 1		/* Use this unless the system bmove is faster */

/* Paranoid settings. Define I_AM_PARANOID if you are paranoid */
#ifdef I_AM_PARANOID
#define DONT_ALLOW_USER_CHANGE 1
#define DONT_USE_MYSQL_PWD 1
#endif

/* #define USE_some_charset 1 was deprecated by changes to configure */
/* my_ctype my_to_upper, my_to_lower, my_sort_order gain theit right value */
/* automagically during configuration */

/* Does the system remember a signal handler after a signal ? */
#ifndef HAVE_BSD_SIGNALS
#define DONT_REMEMBER_SIGNAL
#endif


#if defined(_lint) || defined(FORCE_INIT_OF_VARS)
#define LINT_INIT(var)	var=0			/* No uninitialize-warning */
#define LINT_INIT_STRUCT(var) memset(&var, 0, sizeof(var)) /* No uninitialize-warning */
#else
#define LINT_INIT(var)
#define LINT_INIT_STRUCT(var)
#endif

/* Define some useful general macros */
#if defined(__cplusplus) && defined(__GNUC__)
#define max(a, b)	((a) >? (b))
#define min(a, b)	((a) <? (b))
#elif !defined(max)
#define max(a, b)	((a) > (b) ? (a) : (b))
#define min(a, b)	((a) < (b) ? (a) : (b))
#endif

#if defined(__EMX__) || !defined(HAVE_UINT)
typedef unsigned int uint;
typedef unsigned short ushort;
#endif

#define sgn(a)		(((a) < 0) ? -1 : ((a) > 0) ? 1 : 0)
#define swap(t,a,b)	{ register t dummy; dummy = a; a = b; b = dummy; }
#define test(a)		((a) ? 1 : 0)
#define set_if_bigger(a,b)  { if ((a) < (b)) (a)=(b); }
#define set_if_smaller(a,b) { if ((a) > (b)) (a)=(b); }
#define test_all_bits(a,b) (((a) & (b)) == (b))
#define set_bits(type, bit_count) (sizeof(type)*8 <= (bit_count) ? ~(type) 0 : ((((type) 1) << (bit_count)) - (type) 1))
#define array_elements(A) ((uint) (sizeof(A)/sizeof(A[0])))
#ifndef HAVE_RINT
#define rint(A) floor((A)+0.5)
#endif

/* Define some general constants */
#ifndef TRUE
#define TRUE		(1)	/* Logical true */
#define FALSE		(0)	/* Logical false */
#endif

#if defined(__GNUC__)
#define function_volatile	volatile
#ifndef my_reinterpret_cast
#define my_reinterpret_cast(A) reinterpret_cast<A>
#endif
#define my_const_cast(A) const_cast<A>
#elif !defined(my_reinterpret_cast)
#define my_reinterpret_cast(A) (A)
#define my_const_cast(A) (A)
#endif
#if !defined(__attribute__) && (defined(__cplusplus) || !defined(__GNUC__)  || __GNUC__ == 2 && __GNUC_MINOR__ < 8)
#define __attribute__(A)
#endif

/* From old s-system.h */

/*
  Support macros for non ansi & other old compilers. Since such
  things are no longer supported we do nothing. We keep then since
  some of our code may still be needed to upgrade old customers.
*/
#define _VARARGS(X) X
#define _STATIC_VARARGS(X) X

#if defined(DBUG_ON) && defined(DBUG_OFF)
#undef DBUG_OFF
#endif

#if defined(_lint) && !defined(DBUG_OFF)
#define DBUG_OFF
#endif

#define MIN_ARRAY_SIZE	0	/* Zero or One. Gcc allows zero*/
#define ASCII_BITS_USED 8	/* Bit char used */
#define NEAR_F			/* No near function handling */

/* Some types that is different between systems */

typedef int	File;		/* File descriptor */
#ifndef my_socket_defined
#define my_socket_defined
#if defined(_WIN64)
#define my_socket unsigned long long
#elif defined(_WIN32)
#define my_socket unsigned int
#else
typedef int my_socket;
#endif
#define my_socket_defined
#endif
#ifndef INVALID_SOCKET
#define INVALID_SOCKET -1
#endif
/* Type for fuctions that handles signals */
#define sig_handler RETSIGTYPE
typedef void	(*sig_return)(void);/* Returns type from signal */
#if defined(__GNUC__) && !defined(_lint)
typedef char	pchar;		/* Mixed prototypes can take char */
typedef char	puchar;		/* Mixed prototypes can take char */
typedef char	pbool;		/* Mixed prototypes can take char */
typedef short	pshort;		/* Mixed prototypes can take short int */
typedef float	pfloat;		/* Mixed prototypes can take float */
#else
typedef int	pchar;		/* Mixed prototypes can't take char */
typedef uint	puchar;		/* Mixed prototypes can't take char */
typedef int	pbool;		/* Mixed prototypes can't take char */
typedef int	pshort;		/* Mixed prototypes can't take short int */
typedef double	pfloat;		/* Mixed prototypes can't take float */
#endif
typedef int	(*qsort_cmp)(const void *,const void *);
#ifdef HAVE_mit_thread
#define qsort_t void
#undef QSORT_TYPE_IS_VOID
#define QSORT_TYPE_IS_VOID
#else
#define qsort_t RETQSORTTYPE	/* Broken GCC cant handle typedef !!!! */
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
typedef SOCKET_SIZE_TYPE size_socket;

#ifndef SOCKOPT_OPTLEN_TYPE
#define SOCKOPT_OPTLEN_TYPE size_socket
#endif

/* file create flags */

#ifndef O_SHARE
#define O_SHARE		0	/* Flag to my_open for shared files */
#ifndef O_BINARY
#define O_BINARY	0	/* Flag to my_open for binary files */
#endif
#define FILE_BINARY	0	/* Flag to my_fopen for binary streams */
#ifdef HAVE_FCNTL
#define HAVE_FCNTL_LOCK
#define F_TO_EOF	0L	/* Param to lockf() to lock rest of file */
#endif
#endif /* O_SHARE */
#ifndef O_TEMPORARY
#define O_TEMPORARY	0
#endif
#ifndef O_SHORT_LIVED
#define O_SHORT_LIVED	0
#endif

/* #define USE_RECORD_LOCK	*/

	/* Unsigned types supported by the compiler */
#define UNSINT8			/* unsigned int8 (char) */
#define UNSINT16		/* unsigned int16 */
#define UNSINT32		/* unsigned int32 */

	/* General constants */
#define SC_MAXWIDTH	256	/* Max width of screen (for error messages) */
#define FN_LEN		256	/* Max file name len */
#define FN_HEADLEN	253	/* Max length of filepart of file name */
#define FN_EXTLEN	20	/* Max length of extension (part of FN_LEN) */
#define FN_REFLEN	512	/* Max length of full path-name */
#define FN_EXTCHAR	'.'
#define FN_HOMELIB	'~'	/* ~/ is used as abbrev for home dir */
#define FN_CURLIB	'.'	/* ./ is used as abbrev for current dir */
#define FN_PARENTDIR	".."	/* Parentdirectory; Must be a string */
#define FN_DEVCHAR	':'

#ifndef FN_LIBCHAR
#ifdef __EMX__
#define FN_LIBCHAR	'\\'
#define FN_ROOTDIR	"\\"
#else
#define FN_LIBCHAR	'/'
#define FN_ROOTDIR	"/"
#endif
#define MY_NFILE	1024	/* This is only used to save filenames */
#endif

/* #define EXT_IN_LIBNAME     */
/* #define FN_NO_CASE_SENCE   */
/* #define FN_UPPER_CASE TRUE */

/*
  Io buffer size; Must be a power of 2 and a multiple of 512. May be
  smaller what the disk page size. This influences the speed of the
  isam btree library. eg to big to slow.
*/
#define IO_SIZE			4096
/*
  How much overhead does malloc have. The code often allocates
  something like 1024-MALLOC_OVERHEAD bytes
*/
#define MALLOC_OVERHEAD 8
	/* get memory in huncs */
#define ONCE_ALLOC_INIT		(uint) (4096-MALLOC_OVERHEAD)
	/* Typical record cash */
#define RECORD_CACHE_SIZE	(uint) (64*1024-MALLOC_OVERHEAD)
	/* Typical key cash */
#define KEY_CACHE_SIZE		(uint) (8*1024*1024-MALLOC_OVERHEAD)

	/* Some things that this system doesn't have */

#define ONLY_OWN_DATABASES	/* We are using only databases by monty */
#define NO_PISAM		/* Not needed anymore */
#define NO_MISAM		/* Not needed anymore */
#define NO_HASH			/* Not needed anymore */
#ifdef _WIN32
#define NO_DIR_LIBRARY		/* Not standar dir-library */
#define USE_MY_STAT_STRUCT	/* For my_lib */
#ifdef _MSC_VER
typedef SSIZE_T ssize_t;
#endif
#endif

/* Some things that this system does have */

#ifndef HAVE_ITOA
#define USE_MY_ITOA		/* There is no itoa */
#endif

/* Some defines of functions for portability */

#ifndef HAVE_ATOD
#define atod		atof
#endif
#ifdef USE_MY_ATOF
#define atof		my_atof
extern void		init_my_atof(void);
extern double		my_atof(const char*);
#endif
#undef remove		/* Crashes MySQL on SCO 5.0.0 */
#ifndef _WIN32
#define closesocket(A)	close(A)
#endif
#ifndef ulonglong2double
#define ulonglong2double(A) ((double) (A))
#define my_off_t2double(A)  ((double) (A))
#endif


#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif
#define ulong_to_double(X) ((double) (ulong) (X))
#define SET_STACK_SIZE(X)	/* Not needed on real machines */

#if !defined(HAVE_mit_thread) && !defined(HAVE_STRTOK_R)
#define strtok_r(A,B,C) strtok((A),(B))
#endif

#ifdef HAVE_LINUXTHREADS
/* #define pthread_sigmask(A,B,C) sigprocmask((A),(B),(C)) */
/* #define sigset(A,B) signal((A),(B)) */
#endif

#if defined(_lint) || defined(FORCE_INIT_OF_VARS) || \
    defined(__cplusplus) || !defined(__GNUC__)
#define UNINIT_VAR(x) x= 0
#else
/* GCC specific self-initialization which inhibits the warning. */
#define UNINIT_VAR(x) x= x
#endif

/* Remove some things that mit_thread break or doesn't support */
#if defined(HAVE_mit_thread) && defined(THREAD)
#undef HAVE_PREAD
#undef HAVE_REALPATH
#undef HAVE_MLOCK
#undef HAVE_TEMPNAM				/* Use ours */
#undef HAVE_PTHREAD_SETPRIO
#undef HAVE_FTRUNCATE
#undef HAVE_READLINK
#endif

/* This is from the old m-machine.h file */

#if SIZEOF_LONG_LONG > 4
#define HAVE_LONG_LONG 1
#endif

#if defined(HAVE_LONG_LONG) && !defined(LONGLONG_MIN)
#define LONGLONG_MIN	((long long) 0x8000000000000000LL)
#define LONGLONG_MAX	((long long) 0x7FFFFFFFFFFFFFFFLL)
#endif


#define INT_MIN64       (~0x7FFFFFFFFFFFFFFFLL)
#define INT_MAX64       0x7FFFFFFFFFFFFFFFLL
#define INT_MIN32       (~0x7FFFFFFFL)
#define INT_MAX32       0x7FFFFFFFL
#define UINT_MAX32      0xFFFFFFFFL
#define INT_MIN24       (~0x007FFFFF)
#define INT_MAX24       0x007FFFFF
#define UINT_MAX24      0x00FFFFFF
#define INT_MIN16       (~0x7FFF)
#define INT_MAX16       0x7FFF
#define UINT_MAX16      0xFFFF
#define INT_MIN8        (~0x7F)
#define INT_MAX8        0x7F
#define UINT_MAX8       0xFF

#ifndef ULL
#ifdef HAVE_LONG_LONG
#define ULL(A) A ## ULL
#else
#define ULL(A) A ## UL
#endif
#endif

#if defined(HAVE_LONG_LONG) && !defined(ULONGLONG_MAX)
/* First check for ANSI C99 definition: */
#ifdef ULLONG_MAX
#define ULONGLONG_MAX  ULLONG_MAX
#else
#define ULONGLONG_MAX ((unsigned long long)(~0ULL))
#endif
#endif /* defined (HAVE_LONG_LONG) && !defined(ULONGLONG_MAX)*/

/* From limits.h instead */
#ifndef DBL_MIN
#define DBL_MIN		4.94065645841246544e-324
#define FLT_MIN		((float)1.40129846432481707e-45)
#endif
#ifndef DBL_MAX
#define DBL_MAX		1.79769313486231470e+308
#define FLT_MAX		((float)3.40282346638528860e+38)
#endif

/*
  Max size that must be added to a so that we know Size to make
  adressable obj.
*/
typedef long my_ptrdiff_t;
#define MY_ALIGN(A,L)	(((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A)	MY_ALIGN((A),sizeof(double))
/* Size to make adressable obj. */
#define ALIGN_PTR(A, t) ((t*) MY_ALIGN((A),sizeof(t)))
			 /* Offset of filed f in structure t */
#define OFFSET(t, f)	((size_t)(char *)&((t *)0)->f)
#define ADD_TO_PTR(ptr,size,type) (type) ((unsigned char*) (ptr)+size)
#define PTR_BYTE_DIFF(A,B) (my_ptrdiff_t) ((unsigned char*) (A) - (unsigned char*) (B))

#define NullS		(char *) 0
/* Nowdays we do not support MessyDos */
#ifndef NEAR
#define NEAR				/* Who needs segments ? */
#define FAR				/* On a good machine */
#ifndef HUGE_PTR
#define HUGE_PTR
#endif
#endif
#if defined(__IBMC__) || defined(__IBMCPP__)
#define STDCALL _System _Export
#elif !defined( STDCALL)
#define STDCALL
#endif

/* Typdefs for easyier portability */

#if defined(VOIDTYPE)
typedef void	*gptr;		/* Generic pointer */
#else
typedef char	*gptr;		/* Generic pointer */
#endif
#ifndef HAVE_INT_8_16_32
typedef char	int8;		/* Signed integer >= 8	bits */
typedef short	int16;		/* Signed integer >= 16 bits */
#endif
#ifndef HAVE_UCHAR
typedef unsigned char	uchar;	/* Short for unsigned char */
#endif
typedef unsigned char	uint8;	/* Short for unsigned integer >= 8  bits */
typedef unsigned short	uint16; /* Short for unsigned integer >= 16 bits */

#if SIZEOF_INT == 4
#ifndef HAVE_INT_8_16_32
typedef int		int32;
#endif
typedef unsigned int	uint32; /* Short for unsigned integer >= 32 bits */
#elif SIZEOF_LONG == 4
#ifndef HAVE_INT_8_16_32
typedef long		int32;
#endif
typedef unsigned long	uint32; /* Short for unsigned integer >= 32 bits */
#else
#error "Neither int or long is of 4 bytes width"
#endif

#if !defined(HAVE_ULONG) && !defined(HAVE_LINUXTHREADS) && !defined(__USE_MISC)
typedef unsigned long	ulong;	/* Short for unsigned long */
#endif
#ifndef longlong_defined
#if defined(HAVE_LONG_LONG) && SIZEOF_LONG != 8
typedef unsigned long long int ulonglong; /* ulong or unsigned long long */
typedef long long int longlong;
#else
typedef unsigned long	ulonglong;	/* ulong or unsigned long long */
typedef long		longlong;
#endif
#define longlong_defined
#endif

#ifndef HAVE_INT64
typedef longlong int64;
#endif
#ifndef HAVE_UINT64
typedef ulonglong uint64;
#endif

#ifndef MIN
#define MIN(a,b) (((a) < (b)) ? (a) : (b))
#endif
#ifndef MAX
#define MAX(a,b) (((a) > (b)) ? (a) : (b))
#endif
#define CMP_NUM(a,b)    (((a) < (b)) ? -1 : ((a) == (b)) ? 0 : 1)
#ifdef USE_RAID
/*
  The following is done with a if to not get problems with pre-processors
  with late define evaluation
*/
#if SIZEOF_OFF_T == 4
#define SYSTEM_SIZEOF_OFF_T 4
#else
#define SYSTEM_SIZEOF_OFF_T 8
#endif
#undef  SIZEOF_OFF_T
#define SIZEOF_OFF_T	    8
#else
#define SYSTEM_SIZEOF_OFF_T SIZEOF_OFF_T
#endif /* USE_RAID */

#if SIZEOF_OFF_T > 4
typedef ulonglong my_off_t;
#else
typedef unsigned long my_off_t;
#endif
#define MY_FILEPOS_ERROR	(~(my_off_t) 0)
#ifndef _WIN32
typedef off_t os_off_t;
#endif

#if defined(_WIN32)
#define socket_errno	WSAGetLastError()
#define SOCKET_EINTR	WSAEINTR 
#define SOCKET_EAGAIN	WSAEWOULDBLOCK
#define SOCKET_ENFILE	ENFILE
#define SOCKET_EMFILE	EMFILE
#define SOCKET_EWOULDBLOCK WSAEWOULDBLOCK
#else /* Unix */
#define socket_errno	errno
#define closesocket(A)	close(A)
#define SOCKET_EINTR	EINTR
#define SOCKET_EAGAIN	EAGAIN
#define SOCKET_EWOULDBLOCK EWOULDBLOCK
#define SOCKET_ENFILE	ENFILE
#define SOCKET_EMFILE	EMFILE
#endif

typedef uint8		int7;	/* Most effective integer 0 <= x <= 127 */
typedef short		int15;	/* Most effective integer 0 <= x <= 32767 */
typedef char		*my_string; /* String of characters */
typedef unsigned long	size_s; /* Size of strings (In string-funcs) */
typedef int		myf;	/* Type of MyFlags in my_funcs */
typedef char		my_bool; /* Small bool */
typedef unsigned long long my_ulonglong;
#if !defined(bool) && !defined(bool_defined) && (!defined(HAVE_BOOL) || !defined(__cplusplus))
typedef char		bool;	/* Ordinary boolean values 0 1 */
#endif
	/* Macros for converting *constants* to the right type */
#define INT8(v)		(int8) (v)
#define INT16(v)	(int16) (v)
#define INT32(v)	(int32) (v)
#define MYF(v)		(myf) (v)

/*
  Defines to make it possible to prioritize register assignments. No
  longer that important with modern compilers.
*/
#ifndef USING_X
#define reg1 register
#define reg2 register
#define reg3 register
#define reg4 register
#define reg5 register
#define reg6 register
#define reg7 register
#define reg8 register
#define reg9 register
#define reg10 register
#define reg11 register
#define reg12 register
#define reg13 register
#define reg14 register
#define reg15 register
#define reg16 register
#endif

/* Defines for time function */
#define SCALE_SEC	100
#define SCALE_USEC	10000
#define MY_HOW_OFTEN_TO_ALARM	2	/* How often we want info on screen */
#define MY_HOW_OFTEN_TO_WRITE	1000	/* How often we want info on screen */

#define NOT_FIXED_DEC 31

#if defined(_WIN32) && defined(_MSVC)
#define MYSQLND_LLU_SPEC "%I64u"
#define MYSQLND_LL_SPEC "%I64d"
#ifndef L64
#define L64(x) x##i64
#endif
#else
#define MYSQLND_LLU_SPEC "%llu"
#define MYSQLND_LL_SPEC "%lld"
#ifndef L64
#define L64(x) x##LL
#endif /* L64 */
#endif /* _WIN32 */
/*
** Define-funktions for reading and storing in machine independent format
**  (low byte first)
*/

/* Optimized store functions for Intel x86 */
#define int1store(T,A) *((int8*) (T)) = (A)
#define uint1korr(A)   (*(((uint8*)(A))))
#if defined(__i386__) || defined(_WIN32)
#define sint2korr(A)	(*((int16 *) (A)))
#define sint3korr(A)	((int32) ((((uchar) (A)[2]) & 128) ? \
				  (((uint32) 255L << 24) | \
				   (((uint32) (uchar) (A)[2]) << 16) |\
				   (((uint32) (uchar) (A)[1]) << 8) | \
				   ((uint32) (uchar) (A)[0])) : \
				  (((uint32) (uchar) (A)[2]) << 16) |\
				  (((uint32) (uchar) (A)[1]) << 8) | \
				  ((uint32) (uchar) (A)[0])))
#define sint4korr(A)	(*((long *) (A)))
#define uint2korr(A)	(*((uint16 *) (A)))
#if defined(HAVE_purify) && !defined(_WIN32)
#define uint3korr(A)	(uint32) (((uint32) ((uchar) (A)[0])) +\
				  (((uint32) ((uchar) (A)[1])) << 8) +\
				  (((uint32) ((uchar) (A)[2])) << 16))
#else
/*
   ATTENTION !
   
    Please, note, uint3korr reads 4 bytes (not 3) !
    It means, that you have to provide enough allocated space !
*/
#define uint3korr(A)	(long) (*((unsigned int *) (A)) & 0xFFFFFF)
#endif /* HAVE_purify && !_WIN32 */
#define uint4korr(A)	(*((uint32 *) (A)))
#define uint5korr(A)	((ulonglong)(((uint32) ((uchar) (A)[0])) +\
				    (((uint32) ((uchar) (A)[1])) << 8) +\
				    (((uint32) ((uchar) (A)[2])) << 16) +\
				    (((uint32) ((uchar) (A)[3])) << 24)) +\
				    (((ulonglong) ((uchar) (A)[4])) << 32))
#define uint6korr(A)	((ulonglong)(((uint32)    ((uchar) (A)[0]))          + \
                                     (((uint32)    ((uchar) (A)[1])) << 8)   + \
                                     (((uint32)    ((uchar) (A)[2])) << 16)  + \
                                     (((uint32)    ((uchar) (A)[3])) << 24)) + \
                         (((ulonglong) ((uchar) (A)[4])) << 32) +       \
                         (((ulonglong) ((uchar) (A)[5])) << 40))
#define uint8korr(A)	(*((ulonglong *) (A)))
#define sint8korr(A)	(*((longlong *) (A)))
#define int2store(T,A)	*((uint16*) (T))= (uint16) (A)
#define int3store(T,A)  do { *(T)=  (uchar) ((A));\
                            *(T+1)=(uchar) (((uint) (A) >> 8));\
                            *(T+2)=(uchar) (((A) >> 16)); } while (0)
#define int4store(T,A)	*((long *) (T))= (long) (A)
#define int5store(T,A)  do { *(T)= (uchar)((A));\
                             *((T)+1)=(uchar) (((A) >> 8));\
                             *((T)+2)=(uchar) (((A) >> 16));\
                             *((T)+3)=(uchar) (((A) >> 24)); \
                             *((T)+4)=(uchar) (((A) >> 32)); } while(0)
#define int6store(T,A)  do { *(T)=    (uchar)((A));          \
                             *((T)+1)=(uchar) (((A) >> 8));  \
                             *((T)+2)=(uchar) (((A) >> 16)); \
                             *((T)+3)=(uchar) (((A) >> 24)); \
                             *((T)+4)=(uchar) (((A) >> 32)); \
                             *((T)+5)=(uchar) (((A) >> 40)); } while(0)
#define int8store(T,A)	*((ulonglong *) (T))= (ulonglong) (A)

typedef union {
  double v;
  long m[2];
} doubleget_union;
#define doubleget(V,M)	\
do { doubleget_union _tmp; \
     _tmp.m[0] = *((long*)(M)); \
     _tmp.m[1] = *(((long*) (M))+1); \
     (V) = _tmp.v; } while(0)
#define doublestore(T,V) do { *((long *) T) = ((doubleget_union *)&V)->m[0]; \
			     *(((long *) T)+1) = ((doubleget_union *)&V)->m[1]; \
                         } while (0)
#define float4get(V,M)   do { *((float *) &(V)) = *((float*) (M)); } while(0)
#define float8get(V,M)   doubleget((V),(M))
#define float4store(V,M) memcpy((uchar*) V,(uchar*) (&M),sizeof(float))
#define floatstore(T,V)  memcpy((uchar*)(T), (uchar*)(&V),sizeof(float))
#define floatget(V,M)    memcpy((uchar*) &V,(uchar*) (M),sizeof(float))
#define float8store(V,M) doublestore((V),(M))
#else

/*
  We're here if it's not a IA-32 architecture (Win32 and UNIX IA-32 defines
  were done before)
*/
#define sint2korr(A)	(int16) (((int16) ((uchar) (A)[0])) +\
				 ((int16) ((int16) (A)[1]) << 8))
#define sint3korr(A)	((int32) ((((uchar) (A)[2]) & 128) ? \
				  (((uint32) 255L << 24) | \
				   (((uint32) (uchar) (A)[2]) << 16) |\
				   (((uint32) (uchar) (A)[1]) << 8) | \
				   ((uint32) (uchar) (A)[0])) : \
				  (((uint32) (uchar) (A)[2]) << 16) |\
				  (((uint32) (uchar) (A)[1]) << 8) | \
				  ((uint32) (uchar) (A)[0])))
#define sint4korr(A)	(int32) (((int32) ((uchar) (A)[0])) +\
				(((int32) ((uchar) (A)[1]) << 8)) +\
				(((int32) ((uchar) (A)[2]) << 16)) +\
				(((int32) ((int16) (A)[3]) << 24)))
#define sint8korr(A)	(longlong) uint8korr(A)
#define uint2korr(A)	(uint16) (((uint16) ((uchar) (A)[0])) +\
				  ((uint16) ((uchar) (A)[1]) << 8))
#define uint3korr(A)	(uint32) (((uint32) ((uchar) (A)[0])) +\
				  (((uint32) ((uchar) (A)[1])) << 8) +\
				  (((uint32) ((uchar) (A)[2])) << 16))
#define uint4korr(A)	(uint32) (((uint32) ((uchar) (A)[0])) +\
				  (((uint32) ((uchar) (A)[1])) << 8) +\
				  (((uint32) ((uchar) (A)[2])) << 16) +\
				  (((uint32) ((uchar) (A)[3])) << 24))
#define uint5korr(A)	((ulonglong)(((uint32) ((uchar) (A)[0])) +\
				    (((uint32) ((uchar) (A)[1])) << 8) +\
				    (((uint32) ((uchar) (A)[2])) << 16) +\
				    (((uint32) ((uchar) (A)[3])) << 24)) +\
				    (((ulonglong) ((uchar) (A)[4])) << 32))
#define uint6korr(A)	((ulonglong)(((uint32)    ((uchar) (A)[0]))          + \
                                     (((uint32)    ((uchar) (A)[1])) << 8)   + \
                                     (((uint32)    ((uchar) (A)[2])) << 16)  + \
                                     (((uint32)    ((uchar) (A)[3])) << 24)) + \
                         (((ulonglong) ((uchar) (A)[4])) << 32) +       \
                         (((ulonglong) ((uchar) (A)[5])) << 40))
#define uint8korr(A)	((ulonglong)(((uint32) ((uchar) (A)[0])) +\
				    (((uint32) ((uchar) (A)[1])) << 8) +\
				    (((uint32) ((uchar) (A)[2])) << 16) +\
				    (((uint32) ((uchar) (A)[3])) << 24)) +\
			(((ulonglong) (((uint32) ((uchar) (A)[4])) +\
				    (((uint32) ((uchar) (A)[5])) << 8) +\
				    (((uint32) ((uchar) (A)[6])) << 16) +\
				    (((uint32) ((uchar) (A)[7])) << 24))) <<\
				    32))
#define int2store(T,A)       do { uint def_temp= (uint) (A) ;\
                                  *((uchar*) (T))=  (uchar)(def_temp); \
                                   *((uchar*) (T)+1)=(uchar)((def_temp >> 8)); \
                             } while(0)
#define int3store(T,A)       do { /*lint -save -e734 */\
                                  *((uchar*)(T))=(uchar) ((A));\
                                  *((uchar*) (T)+1)=(uchar) (((A) >> 8));\
                                  *((uchar*)(T)+2)=(uchar) (((A) >> 16)); \
                                  /*lint -restore */} while(0)
#define int4store(T,A)       do { *((char *)(T))=(char) ((A));\
                                  *(((char *)(T))+1)=(char) (((A) >> 8));\
                                  *(((char *)(T))+2)=(char) (((A) >> 16));\
                                  *(((char *)(T))+3)=(char) (((A) >> 24)); } while(0)
#define int5store(T,A)       do { *((char *)(T))=     (char)((A));  \
                                  *(((char *)(T))+1)= (char)(((A) >> 8)); \
                                  *(((char *)(T))+2)= (char)(((A) >> 16)); \
                                  *(((char *)(T))+3)= (char)(((A) >> 24)); \
                                  *(((char *)(T))+4)= (char)(((A) >> 32)); \
		                } while(0)
#define int6store(T,A)       do { *((char *)(T))=     (char)((A)); \
                                  *(((char *)(T))+1)= (char)(((A) >> 8)); \
                                  *(((char *)(T))+2)= (char)(((A) >> 16)); \
                                  *(((char *)(T))+3)= (char)(((A) >> 24)); \
                                  *(((char *)(T))+4)= (char)(((A) >> 32)); \
                                  *(((char *)(T))+5)= (char)(((A) >> 40)); \
                                } while(0)
#define int8store(T,A)       do { uint def_temp= (uint) (A), def_temp2= (uint) ((A) >> 32); \
                                  int4store((T),def_temp); \
                                  int4store((T+4),def_temp2); } while(0)
#ifdef HAVE_BIGENDIAN
#define float4store(T,A) do { *(T)= ((uchar *) &A)[3];\
                              *((T)+1)=(char) ((uchar *) &A)[2];\
                              *((T)+2)=(char) ((uchar *) &A)[1];\
                              *((T)+3)=(char) ((uchar *) &A)[0]; } while(0)

#define float4get(V,M)   do { float def_temp;\
                              ((uchar*) &def_temp)[0]=(M)[3];\
                              ((uchar*) &def_temp)[1]=(M)[2];\
                              ((uchar*) &def_temp)[2]=(M)[1];\
                              ((uchar*) &def_temp)[3]=(M)[0];\
                              (V)=def_temp; } while(0)
#define float8store(T,V) do { *(T)= ((uchar *) &V)[7];\
                              *((T)+1)=(char) ((uchar *) &V)[6];\
                              *((T)+2)=(char) ((uchar *) &V)[5];\
                              *((T)+3)=(char) ((uchar *) &V)[4];\
                              *((T)+4)=(char) ((uchar *) &V)[3];\
                              *((T)+5)=(char) ((uchar *) &V)[2];\
                              *((T)+6)=(char) ((uchar *) &V)[1];\
                              *((T)+7)=(char) ((uchar *) &V)[0]; } while(0)

#define float8get(V,M)   do { double def_temp;\
                              ((uchar*) &def_temp)[0]=(M)[7];\
                              ((uchar*) &def_temp)[1]=(M)[6];\
                              ((uchar*) &def_temp)[2]=(M)[5];\
                              ((uchar*) &def_temp)[3]=(M)[4];\
                              ((uchar*) &def_temp)[4]=(M)[3];\
                              ((uchar*) &def_temp)[5]=(M)[2];\
                              ((uchar*) &def_temp)[6]=(M)[1];\
                              ((uchar*) &def_temp)[7]=(M)[0];\
                              (V) = def_temp; } while(0)
#else
#define float4get(V,M)   memcpy(&V, (M), sizeof(float))
#define float4store(V,M) memcpy(V, (&M), sizeof(float))

#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
#define doublestore(T,V) do { *(((char*)T)+0)=(char) ((uchar *) &V)[4];\
                              *(((char*)T)+1)=(char) ((uchar *) &V)[5];\
                              *(((char*)T)+2)=(char) ((uchar *) &V)[6];\
                              *(((char*)T)+3)=(char) ((uchar *) &V)[7];\
                              *(((char*)T)+4)=(char) ((uchar *) &V)[0];\
                              *(((char*)T)+5)=(char) ((uchar *) &V)[1];\
                              *(((char*)T)+6)=(char) ((uchar *) &V)[2];\
                              *(((char*)T)+7)=(char) ((uchar *) &V)[3]; }\
                         while(0)
#define doubleget(V,M)   do { double def_temp;\
                              ((uchar*) &def_temp)[0]=(M)[4];\
                              ((uchar*) &def_temp)[1]=(M)[5];\
                              ((uchar*) &def_temp)[2]=(M)[6];\
                              ((uchar*) &def_temp)[3]=(M)[7];\
                              ((uchar*) &def_temp)[4]=(M)[0];\
                              ((uchar*) &def_temp)[5]=(M)[1];\
                              ((uchar*) &def_temp)[6]=(M)[2];\
                              ((uchar*) &def_temp)[7]=(M)[3];\
                              (V) = def_temp; } while(0)
#endif /* __FLOAT_WORD_ORDER */

#define float8get(V,M)   doubleget((V),(M))
#define float8store(V,M) doublestore((V),(M))
#endif /* WORDS_BIGENDIAN */

#endif /* __i386__ OR _WIN32 */

/*
  Macro for reading 32-bit integer from network byte order (big-endian)
  from unaligned memory location.
*/
#define int4net(A)        (int32) (((uint32) ((uchar) (A)[3]))        |\
				  (((uint32) ((uchar) (A)[2])) << 8)  |\
				  (((uint32) ((uchar) (A)[1])) << 16) |\
				  (((uint32) ((uchar) (A)[0])) << 24))
/*
  Define-funktions for reading and storing in machine format from/to
  short/long to/from some place in memory V should be a (not
  register) variable, M is a pointer to byte
*/

#ifdef HAVE_BIGENDIAN

#define ushortget(V,M)  do { V = (uint16) (((uint16) ((uchar) (M)[1]))+\
                                 ((uint16) ((uint16) (M)[0]) << 8)); } while(0)
#define shortget(V,M)   do { V = (short) (((short) ((uchar) (M)[1]))+\
                                 ((short) ((short) (M)[0]) << 8)); } while(0)
#define longget(V,M)    do { int32 def_temp;\
                             ((uchar*) &def_temp)[0]=(M)[0];\
                             ((uchar*) &def_temp)[1]=(M)[1];\
                             ((uchar*) &def_temp)[2]=(M)[2];\
                             ((uchar*) &def_temp)[3]=(M)[3];\
                             (V)=def_temp; } while(0)
#define ulongget(V,M)   do { uint32 def_temp;\
                            ((uchar*) &def_temp)[0]=(M)[0];\
                            ((uchar*) &def_temp)[1]=(M)[1];\
                            ((uchar*) &def_temp)[2]=(M)[2];\
                            ((uchar*) &def_temp)[3]=(M)[3];\
                            (V)=def_temp; } while(0)
#define shortstore(T,A) do { uint def_temp=(uint) (A) ;\
                             *(((char*)T)+1)=(char)(def_temp); \
                             *(((char*)T)+0)=(char)(def_temp >> 8); } while(0)
#define longstore(T,A)  do { *(((char*)T)+3)=((A));\
                             *(((char*)T)+2)=(((A) >> 8));\
                             *(((char*)T)+1)=(((A) >> 16));\
                             *(((char*)T)+0)=(((A) >> 24)); } while(0)

#define floatget(V,M)    memcpy(&V, (M), sizeof(float))
#define floatstore(T,V)  memcpy((T), (void*) (&V), sizeof(float))
#define doubleget(V,M)	 memcpy(&V, (M), sizeof(double))
#define doublestore(T,V) memcpy((T), (void *) &V, sizeof(double))
#define longlongget(V,M) memcpy(&V, (M), sizeof(ulonglong))
#define longlongstore(T,V) memcpy((T), &V, sizeof(ulonglong))

#else

#define ushortget(V,M)	do { V = uint2korr(M); } while(0)
#define shortget(V,M)	do { V = sint2korr(M); } while(0)
#define longget(V,M)	do { V = sint4korr(M); } while(0)
#define ulongget(V,M)   do { V = uint4korr(M); } while(0)
#define shortstore(T,V) int2store(T,V)
#define longstore(T,V)	int4store(T,V)
#ifndef floatstore
#define floatstore(T,V)  memcpy((T), (void *) (&V), sizeof(float))
#define floatget(V,M)    memcpy(&V, (M), sizeof(float))
#endif
#ifndef doubleget
#define doubleget(V,M)	 memcpy(&V, (M), sizeof(double))
#define doublestore(T,V) memcpy((T), (void *) &V, sizeof(double))
#endif /* doubleget */
#define longlongget(V,M) memcpy(&V, (M), sizeof(ulonglong))
#define longlongstore(T,V) memcpy((T), &V, sizeof(ulonglong))

#endif /* WORDS_BIGENDIAN */

#ifndef THREAD
#define thread_safe_increment(V,L) (V)++
#define thread_safe_add(V,C,L)     (V)+=(C)
#define thread_safe_sub(V,C,L)     (V)-=(C)
#define statistic_increment(V,L)   (V)++
#define statistic_add(V,C,L)       (V)+=(C)
#endif

#ifdef _WIN32
#define SO_EXT ".dll"
#else
#define SO_EXT ".so"
#endif

#ifndef DBUG_OFF
#define dbug_assert(A) assert(A)
#define DBUG_ASSERT(A) assert(A)
#else
#define dbug_assert(A)
#define DBUG_ASSERT(A)
#endif

#ifdef HAVE_DLOPEN
#ifdef _WIN32
#define dlsym(lib, name) GetProcAddress((HMODULE)lib, name)
#define dlopen(libname, unused) LoadLibraryEx(libname, NULL, 0)
#define dlclose(lib) FreeLibrary((HMODULE)lib)
#elif defined(HAVE_DLFCN_H)
#include <dlfcn.h>
#endif
#ifndef HAVE_DLERROR
#define dlerror() ""
#endif
#endif

#if SIZEOF_CHARP == SIZEOF_INT
typedef unsigned int intptr;
#elif SIZEOF_CHARP == SIZEOF_LONG
typedef unsigned long intptr;
#elif SIZEOF_CHARP == SIZEOF_LONG_LONG
typedef unsigned long long intptr;
#else
#error sizeof(void *) is not sizeof(int, long or long long)
#endif

#ifdef _WIN32
#define IF_WIN(A,B) A 
#else
#define IF_WIN(A,B) B 
#endif

#ifndef RTLD_NOW
#define RTLD_NOW 1
#endif

#endif /* _global_h */
