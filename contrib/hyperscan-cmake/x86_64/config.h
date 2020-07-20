/* used by cmake */

#ifndef CONFIG_H_
#define CONFIG_H_

/* "Define if the build is 32 bit" */
/* #undef ARCH_32_BIT */

/* "Define if the build is 64 bit" */
#define ARCH_64_BIT

/* "Define if building for IA32" */
/* #undef ARCH_IA32 */

/* "Define if building for EM64T" */
#define ARCH_X86_64

/* internal build, switch on dump support. */
/* #undef DUMP_SUPPORT */

/* Define if building "fat" runtime. */
/* #undef FAT_RUNTIME */

/* Define if building AVX-512 in the fat runtime. */
/* #undef BUILD_AVX512 */

/* Define to 1 if `backtrace' works. */
#define HAVE_BACKTRACE

/* C compiler has __builtin_assume_aligned */
#define HAVE_CC_BUILTIN_ASSUME_ALIGNED

/* C++ compiler has __builtin_assume_aligned */
#define HAVE_CXX_BUILTIN_ASSUME_ALIGNED

/* C++ compiler has x86intrin.h */
#define HAVE_CXX_X86INTRIN_H

/* C compiler has x86intrin.h */
#define HAVE_C_X86INTRIN_H

/* C++ compiler has intrin.h */
/* #undef HAVE_CXX_INTRIN_H */

/* C compiler has intrin.h */
/* #undef HAVE_C_INTRIN_H */

/* Define to 1 if you have the declaration of `pthread_setaffinity_np', and to
   0 if you don't. */
/* #undef HAVE_DECL_PTHREAD_SETAFFINITY_NP */

/* #undef HAVE_PTHREAD_NP_H */

/* Define to 1 if you have the `malloc_info' function. */
/* #undef HAVE_MALLOC_INFO */

/* Define to 1 if you have the `memmem' function. */
/* #undef HAVE_MEMMEM */

/* Define to 1 if you have a working `mmap' system call. */
#define HAVE_MMAP

/* Define to 1 if `posix_memalign' works. */
#define HAVE_POSIX_MEMALIGN

/* Define to 1 if you have the `setrlimit' function. */
#define HAVE_SETRLIMIT

/* Define to 1 if you have the `shmget' function. */
/* #undef HAVE_SHMGET */

/* Define to 1 if you have the `sigaction' function. */
#define HAVE_SIGACTION

/* Define to 1 if you have the `sigaltstack' function. */
#define HAVE_SIGALTSTACK

/* Define if the sqlite3_open_v2 call is available */
/* #undef HAVE_SQLITE3_OPEN_V2 */

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H

/* Define to 1 if you have the `_aligned_malloc' function. */
/* #undef HAVE__ALIGNED_MALLOC */

/* Define if compiler has __builtin_constant_p */
#define HAVE__BUILTIN_CONSTANT_P

/* Optimize, inline critical functions */
#define HS_OPTIMIZE

#define HS_VERSION
#define HS_MAJOR_VERSION
#define HS_MINOR_VERSION
#define HS_PATCH_VERSION

#define BUILD_DATE

/* define if this is a release build. */
#define RELEASE_BUILD

/* define if reverse_graph requires patch for boost 1.62.0 */
/* #undef BOOST_REVGRAPH_PATCH */

#endif /* CONFIG_H_ */
