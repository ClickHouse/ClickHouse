/* src/config.h.  Generated from config.h.in by configure.  */
/* src/config.h.in.  Generated from configure.ac by autoheader.  */


#ifndef GPERFTOOLS_CONFIG_H_
#define GPERFTOOLS_CONFIG_H_


/* Build runtime detection for sized delete */
/* #undef ENABLE_DYNAMIC_SIZED_DELETE */

/* Build sized deletion operators */
/* #undef ENABLE_SIZED_DELETE */

/* Define to 1 if compiler supports __builtin_expect */
#if _MSC_VER
#define HAVE_BUILTIN_EXPECT 0
#else
#define HAVE_BUILTIN_EXPECT 1
#endif

/* Define to 1 if compiler supports __builtin_stack_pointer */
/* #undef HAVE_BUILTIN_STACK_POINTER */

/* Define to 1 if you have the <conflict-signal.h> header file. */
/* #undef HAVE_CONFLICT_SIGNAL_H */

/* Define to 1 if you have the <cygwin/signal.h> header file. */
/* #undef HAVE_CYGWIN_SIGNAL_H */

/* Define to 1 if you have the declaration of `backtrace', and to 0 if you
   don't. */
/* #undef HAVE_DECL_BACKTRACE */

/* Define to 1 if you have the declaration of `cfree', and to 0 if you don't.
   */
#define HAVE_DECL_CFREE 1

/* Define to 1 if you have the declaration of `memalign', and to 0 if you
   don't. */
#define HAVE_DECL_MEMALIGN 1

/* Define to 1 if you have the declaration of `nanosleep', and to 0 if you
   don't. */
/* #undef HAVE_DECL_NANOSLEEP */

/* Define to 1 if you have the declaration of `posix_memalign', and to 0 if
   you don't. */
#define HAVE_DECL_POSIX_MEMALIGN 1

/* Define to 1 if you have the declaration of `pvalloc', and to 0 if you
   don't. */
#define HAVE_DECL_PVALLOC 1

/* Define to 1 if you have the declaration of `sleep', and to 0 if you don't.
   */
/* #undef HAVE_DECL_SLEEP */

/* Define to 1 if you have the declaration of `uname', and to 0 if you don't.
   */
#define HAVE_DECL_UNAME 1

/* Define to 1 if you have the declaration of `valloc', and to 0 if you don't.
   */
#define HAVE_DECL_VALLOC 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if the system has the type `Elf32_Versym'. */
#define HAVE_ELF32_VERSYM 1

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the <features.h> header file. */
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#define HAVE_FEATURES_H 1
#endif

/* Define to 1 if you have the `fork' function. */
#define HAVE_FORK 1

/* Define to 1 if you have the `geteuid' function. */
#define HAVE_GETEUID 1

/* Define to 1 if you have the `getpagesize' function. */
#define HAVE_GETPAGESIZE 1

/* Define to 1 if you have the <glob.h> header file. */
#define HAVE_GLOB_H 1

/* Define to 1 if you have the <grp.h> header file. */
#define HAVE_GRP_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the <libunwind.h> header file. */
//#define HAVE_LIBUNWIND_H 1

/* Define to 1 if you have the <linux/ptrace.h> header file. */
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#define HAVE_LINUX_PTRACE_H 1
#endif

/* Define if this is Linux that has SIGEV_THREAD_ID */
#define HAVE_LINUX_SIGEV_THREAD_ID 1

/* Define to 1 if you have the <malloc.h> header file. */
#if !defined(__FreeBSD__)
#define HAVE_MALLOC_H 1
#endif

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have a working `mmap' system call. */
#define HAVE_MMAP 1

/* define if the compiler implements namespaces */
#define HAVE_NAMESPACES 1

/* Define to 1 if you have the <poll.h> header file. */
#define HAVE_POLL_H 1

/* define if libc has program_invocation_name */
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#define HAVE_PROGRAM_INVOCATION_NAME 1
#endif

/* Define if you have POSIX threads libraries and header files. */
#define HAVE_PTHREAD 1

/* defined to 1 if pthread symbols are exposed even without include pthread.h
   */
/* #undef HAVE_PTHREAD_DESPITE_ASKING_FOR */

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define to 1 if you have the `sbrk' function. */
#define HAVE_SBRK 1

/* Define to 1 if you have the <sched.h> header file. */
#define HAVE_SCHED_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if the system has the type `struct mallinfo'. */
//#if !defined(__APPLE__) && !defined(__FreeBSD__)
#if !defined(__APPLE__)
#define HAVE_STRUCT_MALLINFO 1
#endif

/* Define to 1 if you have the <sys/cdefs.h> header file. */
#define HAVE_SYS_CDEFS_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/prctl.h> header file. */
#define HAVE_SYS_PRCTL_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/syscall.h> header file. */
#define HAVE_SYS_SYSCALL_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/ucontext.h> header file. */
/* #undef HAVE_SYS_UCONTEXT_H */

/* Define to 1 if you have the <sys/wait.h> header file. */
#define HAVE_SYS_WAIT_H 1

/* Define to 1 if compiler supports __thread */
#define HAVE_TLS 1

/* Define to 1 if you have the <ucontext.h> header file. */
/* #undef HAVE_UCONTEXT_H */

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Whether <unwind.h> contains _Unwind_Backtrace */
#define HAVE_UNWIND_BACKTRACE 1

/* Define to 1 if you have the <unwind.h> header file. */
#define HAVE_UNWIND_H 1

/* Define to 1 if you have the <valgrind.h> header file. */
/* #undef HAVE_VALGRIND_H */

/* define if your compiler has __attribute__ */
#define HAVE___ATTRIBUTE__ 1

/* Define to 1 if compiler supports __environ */
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#define HAVE___ENVIRON 1
#endif

/* Define to 1 if the system has the type `__int64'. */
/* #undef HAVE___INT64 */

/* prefix where we look for installed files */
#define INSTALL_PREFIX "/usr/local"

/* Define to 1 if int32_t is equivalent to intptr_t */
/* #undef INT32_EQUALS_INTPTR */

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
#define LT_OBJDIR ".libs/"

/* Name of package */
#define PACKAGE "gperftools"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "gperftools@googlegroups.com"

/* Define to the full name of this package. */
#define PACKAGE_NAME "gperftools"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "gperftools 2.5"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "gperftools"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "2.5"

/* How to access the PC from a struct ucontext */
/* #undef PC_FROM_UCONTEXT */

/* Always the empty-string on non-windows systems. On windows, should be
   "__declspec(dllexport)". This way, when we compile the dll, we export our
   functions/classes. It's safe to define this here because config.h is only
   used internally, to compile the DLL, and every DLL source file #includes
   "config.h" before anything else. */
#define PERFTOOLS_DLL_DECL /**/

/* printf format code for printing a size_t and ssize_t */
#define PRIdS "ld"

/* printf format code for printing a size_t and ssize_t */
#define PRIuS "lu"

/* printf format code for printing a size_t and ssize_t */
#define PRIxS "lx"

/* Mark the systems where we know it's bad if pthreads runs too
   early before main (before threads are initialized, presumably).  */
#ifdef __FreeBSD__
#define PTHREADS_CRASHES_IF_RUN_TOO_EARLY 1
#endif

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* the namespace where STL code like vector<> is defined */
#define STL_NAMESPACE std

/* Define 32K of internal pages size for tcmalloc */
/* #undef TCMALLOC_32K_PAGES */

/* Define 64K of internal pages size for tcmalloc */
/* #undef TCMALLOC_64K_PAGES */

/* Define 8 bytes of allocation alignment for tcmalloc */
/* #undef TCMALLOC_ALIGN_8BYTES */

/* Version number of package */
#define VERSION "2.5"

/* C99 says: define this to get the PRI... macros from stdint.h */
#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS 1
#endif

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif


#ifdef __MINGW32__
#include "windows/mingw.h"
#endif

#endif  /* #ifndef GPERFTOOLS_CONFIG_H_ */

