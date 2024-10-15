/* src/include/pg_config.h.  Generated from pg_config.h.in by configure.  */
/* src/include/pg_config.h.in.  Generated from configure.in by autoheader.  */

/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* The normal alignment of `double', in bytes. */
#define ALIGNOF_DOUBLE 4

/* The normal alignment of `int', in bytes. */
#define ALIGNOF_INT 4

/* The normal alignment of `long', in bytes. */
#define ALIGNOF_LONG 4

/* The normal alignment of `long long int', in bytes. */
#define ALIGNOF_LONG_LONG_INT 4

/* The normal alignment of `short', in bytes. */
#define ALIGNOF_SHORT 2

/* Size of a disk block --- this also limits the size of a tuple. You can set
   it bigger if you need bigger tuples (although TOAST should reduce the need
   to have large tuples, since fields can be spread across multiple tuples).
   BLCKSZ must be a power of 2. The maximum possible value of BLCKSZ is
   currently 2^15 (32768). This is determined by the 15-bit widths of the
   lp_off and lp_len fields in ItemIdData (see include/storage/itemid.h).
   Changing BLCKSZ requires an initdb. */
#define BLCKSZ 8192

/* Define to the default TCP port number on which the server listens and to
   which clients will try to connect. This can be overridden at run-time, but
   it's convenient if your clients have the right default compiled in.
   (--with-pgport=PORTNUM) */
#define DEF_PGPORT 5432

/* Define to the default TCP port number as a string constant. */
#define DEF_PGPORT_STR "5432"

/* Define to the file name extension of dynamically-loadable modules. */
#define DLSUFFIX ".so"

/* Define to build with GSSAPI support. (--with-gssapi) */
//#define ENABLE_GSS 0

/* Define to 1 if you want National Language Support. (--enable-nls) */
/* #undef ENABLE_NLS */

/* Define to 1 to build client libraries as thread-safe code.
   (--enable-thread-safety) */
#define ENABLE_THREAD_SAFETY 1

/* Define to nothing if C supports flexible array members, and to 1 if it does
   not. That way, with a declaration like `struct s { int n; double
   d[FLEXIBLE_ARRAY_MEMBER]; };', the struct hack can be used with pre-C99
   compilers. When computing the size of such an object, don't use 'sizeof
   (struct s)' as it overestimates the size. Use 'offsetof (struct s, d)'
   instead. Don't use 'offsetof (struct s, d[0])', as this doesn't work with
   MSVC and with C++ compilers. */
#define FLEXIBLE_ARRAY_MEMBER /**/

/* float4 values are passed by value if 'true', by reference if 'false' */
#define FLOAT4PASSBYVAL true

/* float8, int8, and related values are passed by value if 'true', by
   reference if 'false' */
#define FLOAT8PASSBYVAL false

/* Define to 1 if you have the `append_history' function. */
/* #undef HAVE_APPEND_HISTORY */

/* Define to 1 if you want to use atomics if available. */
#define HAVE_ATOMICS 1

/* Define to 1 if you have the <atomic.h> header file. */
/* #undef HAVE_ATOMIC_H */

/* Define to 1 if you have the `cbrt' function. */
#define HAVE_CBRT 1

/* Define to 1 if you have the `class' function. */
/* #undef HAVE_CLASS */

/* Define to 1 if you have the <crtdefs.h> header file. */
/* #undef HAVE_CRTDEFS_H */

/* Define to 1 if you have the `crypt' function. */
#define HAVE_CRYPT 1

/* Define to 1 if you have the <crypt.h> header file. */
#define HAVE_CRYPT_H 1

/* Define to 1 if you have the declaration of `fdatasync', and to 0 if you
   don't. */
#define HAVE_DECL_FDATASYNC 1

/* Define to 1 if you have the declaration of `F_FULLFSYNC', and to 0 if you
   don't. */
#define HAVE_DECL_F_FULLFSYNC 0

/* Define to 1 if you have the declaration of `posix_fadvise', and to 0 if you
   don't. */
#define HAVE_DECL_POSIX_FADVISE 1

/* Define to 1 if you have the declaration of `snprintf', and to 0 if you
   don't. */
#define HAVE_DECL_SNPRINTF 1

/* Define to 1 if you have the declaration of `strlcat', and to 0 if you
   don't. */
#if OS_DARWIN
#define HAVE_DECL_STRLCAT 1
#endif

/* Define to 1 if you have the declaration of `strlcpy', and to 0 if you
   don't. */
#if OS_DARWIN
#define HAVE_DECL_STRLCPY 1
#endif

/* Define to 1 if you have the declaration of `sys_siglist', and to 0 if you
   don't. */
#define HAVE_DECL_SYS_SIGLIST 1

/* Define to 1 if you have the declaration of `vsnprintf', and to 0 if you
   don't. */
#define HAVE_DECL_VSNPRINTF 1

/* Define to 1 if you have the <dld.h> header file. */
/* #undef HAVE_DLD_H */

/* Define to 1 if you have the <editline/history.h> header file. */
/* #undef HAVE_EDITLINE_HISTORY_H */

/* Define to 1 if you have the <editline/readline.h> header file. */
#define HAVE_EDITLINE_READLINE_H 1

/* Define to 1 if you have the `fpclass' function. */
/* #undef HAVE_FPCLASS */

/* Define to 1 if you have the `fp_class' function. */
/* #undef HAVE_FP_CLASS */

/* Define to 1 if you have the `fp_class_d' function. */
/* #undef HAVE_FP_CLASS_D */

/* Define to 1 if you have the <fp_class.h> header file. */
/* #undef HAVE_FP_CLASS_H */

/* Define to 1 if fseeko (and presumably ftello) exists and is declared. */
#define HAVE_FSEEKO 1

/* Define to 1 if you have __atomic_compare_exchange_n(int *, int *, int). */
/* #undef HAVE_GCC__ATOMIC_INT32_CAS */

/* Define to 1 if you have __atomic_compare_exchange_n(int64 *, int *, int64).
   */
/* #undef HAVE_GCC__ATOMIC_INT64_CAS */

/* Define to 1 if you have __sync_lock_test_and_set(char *) and friends. */
#define HAVE_GCC__SYNC_CHAR_TAS 1

/* Define to 1 if you have __sync_compare_and_swap(int *, int, int). */
/* #undef HAVE_GCC__SYNC_INT32_CAS */

/* Define to 1 if you have __sync_lock_test_and_set(int *) and friends. */
#define HAVE_GCC__SYNC_INT32_TAS 1

/* Define to 1 if you have __sync_compare_and_swap(int64 *, int64, int64). */
/* #undef HAVE_GCC__SYNC_INT64_CAS */

/* Define to 1 if you have the `getifaddrs' function. */
#define HAVE_GETIFADDRS 1

/* Define to 1 if you have the `getopt' function. */
#define HAVE_GETOPT 1

/* Define to 1 if you have the <getopt.h> header file. */
#define HAVE_GETOPT_H 1

/* Define to 1 if you have the `getopt_long' function. */
#define HAVE_GETOPT_LONG 1

/* Define to 1 if you have the `getpeereid' function. */
/* #undef HAVE_GETPEEREID */

/* Define to 1 if you have the `getpeerucred' function. */
/* #undef HAVE_GETPEERUCRED */

/* Define to 1 if you have the <gssapi_ext.h> header file. */
/* #undef HAVE_GSSAPI_EXT_H */

/* Define to 1 if you have the <gssapi/gssapi_ext.h> header file. */
/* #undef HAVE_GSSAPI_GSSAPI_EXT_H */

/* Define to 1 if you have the <gssapi/gssapi.h> header file. */
//#define HAVE_GSSAPI_GSSAPI_H 0

/* Define to 1 if you have the <gssapi.h> header file. */
/* #undef HAVE_GSSAPI_H */

/* Define to 1 if you have the <history.h> header file. */
/* #undef HAVE_HISTORY_H */

/* Define to 1 if you have the `history_truncate_file' function. */
#define HAVE_HISTORY_TRUNCATE_FILE 1

/* Define to 1 if you have the <ieeefp.h> header file. */
/* #undef HAVE_IEEEFP_H */

/* Define to 1 if you have the <ifaddrs.h> header file. */
#define HAVE_IFADDRS_H 1

/* Define to 1 if you have the `inet_aton' function. */
#define HAVE_INET_ATON 1

/* Define to 1 if you have the `inet_pton' function. */
#define HAVE_INET_PTON 1

/* Define to 1 if the system has the type `int64'. */
/* #undef HAVE_INT64 */

/* Define to 1 if the system has the type `int8'. */
/* #undef HAVE_INT8 */

/* Define to 1 if the system has the type `intptr_t'. */
#define HAVE_INTPTR_T 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the global variable 'int opterr'. */
#define HAVE_INT_OPTERR 1

/* Define to 1 if you have the global variable 'int optreset'. */
/* #undef HAVE_INT_OPTRESET */

/* Define to 1 if you have the global variable 'int timezone'. */
#define HAVE_INT_TIMEZONE 1

/* Define to 1 if you have isinf(). */
#define HAVE_ISINF 1

/* Define to 1 if you have the <langinfo.h> header file. */
#define HAVE_LANGINFO_H 1

/* Define to 1 if you have the `crypto' library (-lcrypto). */
#define HAVE_LIBCRYPTO 1

/* Define to 1 if you have the `ldap' library (-lldap). */
//#define HAVE_LIBLDAP 0

/* Define to 1 if you have the `m' library (-lm). */
#define HAVE_LIBM 1

/* Define to 1 if you have the `pam' library (-lpam). */
#define HAVE_LIBPAM 1

/* Define if you have a function readline library */
#define HAVE_LIBREADLINE 1

/* Define to 1 if you have the `selinux' library (-lselinux). */
/* #undef HAVE_LIBSELINUX */

/* Define to 1 if you have the `ssl' library (-lssl). */
#define HAVE_LIBSSL 0

/* Define to 1 if you have the `wldap32' library (-lwldap32). */
/* #undef HAVE_LIBWLDAP32 */

/* Define to 1 if you have the `xml2' library (-lxml2). */
#define HAVE_LIBXML2 1

/* Define to 1 if you have the `xslt' library (-lxslt). */
#define HAVE_LIBXSLT 1

/* Define to 1 if you have the `z' library (-lz). */
#define HAVE_LIBZ 1

/* Define to 1 if you have the `zstd' library (-lzstd). */
/* #undef HAVE_LIBZSTD */

/* Define to 1 if constants of type 'long long int' should have the suffix LL.
   */
#define HAVE_LL_CONSTANTS 1

/* Define to 1 if the system has the type `locale_t'. */
#define HAVE_LOCALE_T 1

/* Define to 1 if `long int' works and is 64 bits. */
/* #undef HAVE_LONG_INT_64 */

/* Define to 1 if the system has the type `long long int'. */
#define HAVE_LONG_LONG_INT 1

/* Define to 1 if `long long int' works and is 64 bits. */
#define HAVE_LONG_LONG_INT_64 1

/* Define to 1 if you have the <mbarrier.h> header file. */
/* #undef HAVE_MBARRIER_H */

/* Define to 1 if you have the `mbstowcs_l' function. */
/* #undef HAVE_MBSTOWCS_L */

/* Define to 1 if you have the `memmove' function. */
#define HAVE_MEMMOVE 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mkdtemp' function. */
#define HAVE_MKDTEMP 1

/* Define to 1 if you have the <net/if.h> header file. */
#define HAVE_NET_IF_H 1

/* Define to 1 if you have the <ossp/uuid.h> header file. */
/* #undef HAVE_OSSP_UUID_H */

/* Define to 1 if you have the <pam/pam_appl.h> header file. */
/* #undef HAVE_PAM_PAM_APPL_H */

/* Define to 1 if you have the `posix_fadvise' function. */
#define HAVE_POSIX_FADVISE 1

/* Define to 1 if you have the declaration of `preadv', and to 0 if you don't. */
/* #undef HAVE_DECL_PREADV */

/* Define to 1 if you have the declaration of `pwritev', and to 0 if you don't. */
/* #define HAVE_DECL_PWRITEV */

/* Define to 1 if you have the `X509_get_signature_info' function. */
/* #undef HAVE_X509_GET_SIGNATURE_INFO */

/* Define to 1 if you have the POSIX signal interface. */
#define HAVE_POSIX_SIGNALS 1

/* Define to 1 if the assembler supports PPC's LWARX mutex hint bit. */
/* #undef HAVE_PPC_LWARX_MUTEX_HINT */

/* Define to 1 if you have the `pthread_is_threaded_np' function. */
/* #undef HAVE_PTHREAD_IS_THREADED_NP */

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define to 1 if you have the <readline.h> header file. */
/* #undef HAVE_READLINE_H */

/* Define to 1 if you have the <readline/history.h> header file. */
#define HAVE_READLINE_HISTORY_H 1

/* Define to 1 if you have the <readline/readline.h> header file. */
/* #undef HAVE_READLINE_READLINE_H */

/* Define to 1 if you have the `rint' function. */
#define HAVE_RINT 1

/* Define to 1 if you have the `rl_completion_matches' function. */
#define HAVE_RL_COMPLETION_MATCHES 1

/* Define to 1 if you have the `rl_filename_completion_function' function. */
#define HAVE_RL_FILENAME_COMPLETION_FUNCTION 1

/* Define to 1 if you have the `rl_reset_screen_size' function. */
/* #undef HAVE_RL_RESET_SCREEN_SIZE */

/* Define to 1 if you have the `rl_variable_bind' function. */
#define HAVE_RL_VARIABLE_BIND 1

/* Define to 1 if you have the <security/pam_appl.h> header file. */
#define HAVE_SECURITY_PAM_APPL_H 1

/* Define to 1 if you have the `setproctitle' function. */
/* #undef HAVE_SETPROCTITLE */

/* Define to 1 if the system has the type `socklen_t'. */
#define HAVE_SOCKLEN_T 1

/* Define to 1 if you have the `sigprocmask' function. */
#define HAVE_SIGPROCMASK 1

/* Define to 1 if you have sigsetjmp(). */
#define HAVE_SIGSETJMP 1

/* Define to 1 if the system has the type `sig_atomic_t'. */
#define HAVE_SIG_ATOMIC_T 1

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1

/* Define to 1 if you have spinlocks. */
#define HAVE_SPINLOCKS 1

/* Define to 1 if you have the `SSL_CTX_set_cert_cb' function. */
#define HAVE_SSL_CTX_SET_CERT_CB 1

/* Define to 1 if you have the `SSL_CTX_set_num_tickets' function. */
/* #define HAVE_SSL_CTX_SET_NUM_TICKETS */

/* Define to 1 if you have the `SSL_get_current_compression' function. */
#define HAVE_SSL_GET_CURRENT_COMPRESSION 0

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strerror' function. */
#define HAVE_STRERROR 1

/* Define to 1 if you have the `strerror_r' function. */
#define HAVE_STRERROR_R 1

/* Define to 1 if you have the <strings.h> header file. */
//#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strlcat' function. */
/* #undef HAVE_STRLCAT */

/* Define to 1 if you have the `strlcpy' function. */
/* #undef HAVE_STRLCPY */

#if (!OS_DARWIN)
#define HAVE_STRCHRNUL 1
#endif

/* Define to 1 if the system has the type `struct option'. */
#define HAVE_STRUCT_OPTION 1

/* Define to 1 if `sa_len' is a member of `struct sockaddr'. */
/* #undef HAVE_STRUCT_SOCKADDR_SA_LEN */

/* Define to 1 if `tm_zone' is a member of `struct tm'. */
#define HAVE_STRUCT_TM_TM_ZONE 1

/* Define to 1 if you have the `sync_file_range' function. */
/* #undef HAVE_SYNC_FILE_RANGE */

/* Define to 1 if you have the syslog interface. */
#define HAVE_SYSLOG 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/personality.h> header file. */
/* #undef HAVE_SYS_PERSONALITY_H */

/* Define to 1 if you have the <sys/poll.h> header file. */
#define HAVE_SYS_POLL_H 1

/* Define to 1 if you have the <sys/signalfd.h> header file. */
/* #undef HAVE_SYS_SIGNALFD_H */

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/ucred.h> header file. */
#if (OS_DARWIN || OS_FREEBSD)
#define HAVE_SYS_UCRED_H 1
#endif

/* Define to 1 if you have the <sys/un.h> header file. */
#define _GNU_SOURCE 1 /* Needed for glibc struct ucred */

/* Define to 1 if you have the <termios.h> header file. */
#define HAVE_TERMIOS_H 1

/* Define to 1 if your `struct tm' has `tm_zone'. Deprecated, use
   `HAVE_STRUCT_TM_TM_ZONE' instead. */
#define HAVE_TM_ZONE 1

/* Define to 1 if you have the `towlower' function. */
#define HAVE_TOWLOWER 1

/* Define to 1 if you have the external array `tzname'. */
#define HAVE_TZNAME 1

/* Define to 1 if you have the <ucred.h> header file. */
/* #undef HAVE_UCRED_H */

/* Define to 1 if the system has the type `uint64'. */
/* #undef HAVE_UINT64 */

/* Define to 1 if the system has the type `uint8'. */
/* #undef HAVE_UINT8 */

/* Define to 1 if the system has the type `uintptr_t'. */
#define HAVE_UINTPTR_T 1

/* Define to 1 if the system has the type `union semun'. */
/* #undef HAVE_UNION_SEMUN */

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have unix sockets. */
#define HAVE_UNIX_SOCKETS 1

/* Define to 1 if the system has the type `unsigned long long int'. */
#define HAVE_UNSIGNED_LONG_LONG_INT 1

/* Define to 1 if you have the `utime' function. */
#define HAVE_UTIME 1

/* Define to 1 if you have the `utimes' function. */
#define HAVE_UTIMES 1

/* Define to 1 if you have the <utime.h> header file. */
#define HAVE_UTIME_H 1

/* Define to 1 if you have BSD UUID support. */
/* #undef HAVE_UUID_BSD */

/* Define to 1 if you have E2FS UUID support. */
/* #undef HAVE_UUID_E2FS */

/* Define to 1 if you have the <uuid.h> header file. */
#define HAVE_UUID_H 1

/* Define to 1 if you have OSSP UUID support. */
#define HAVE_UUID_OSSP 1

/* Define to 1 if you have the <uuid/uuid.h> header file. */
/* #undef HAVE_UUID_UUID_H */

/* Define to 1 if your compiler knows the visibility("hidden") attribute. */
/* #undef HAVE_VISIBILITY_ATTRIBUTE */

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1

/* Define to 1 if you have the <wchar.h> header file. */
#define HAVE_WCHAR_H 1

/* Define to 1 if you have the `wcstombs' function. */
#define HAVE_WCSTOMBS 1

/* Define to 1 if you have the `wcstombs_l' function. */
/* #undef HAVE_WCSTOMBS_L */

/* Define to 1 if your compiler understands __builtin_bswap32. */
/* #undef HAVE__BUILTIN_BSWAP32 */

/* Define to 1 if your compiler understands __builtin_constant_p. */
#define HAVE__BUILTIN_CONSTANT_P 1

/* Define to 1 if your compiler understands __builtin_frame_address. */
/* #undef HAVE__BUILTIN_FRAME_ADDRESS */

/* Define to 1 if your compiler understands __builtin_types_compatible_p. */
#define HAVE__BUILTIN_TYPES_COMPATIBLE_P 1

/* Define to 1 if your compiler understands __builtin_unreachable. */
/* #undef HAVE__BUILTIN_UNREACHABLE */

/* Define to 1 if you have __cpuid. */
/* #undef HAVE__CPUID */

/* Define to 1 if you have __get_cpuid. */
/* #undef HAVE__GET_CPUID */

/* Define to 1 if your compiler understands _Static_assert. */
/* #undef HAVE__STATIC_ASSERT */

/* Define to 1 if your compiler understands __VA_ARGS__ in macros. */
#define HAVE__VA_ARGS 1

/* Define to the appropriate snprintf length modifier for 64-bit ints. */
#define INT64_MODIFIER "ll"

/* Define to 1 if `locale_t' requires <xlocale.h>. */
/* #undef LOCALE_T_IN_XLOCALE */

/* Define as the maximum alignment requirement of any C data type. */
#define MAXIMUM_ALIGNOF 4

/* Define bytes to use libc memset(). */
#define MEMSET_LOOP_LIMIT 1024

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "pgsql-bugs@postgresql.org"

/* Define to the full name of this package. */
#define PACKAGE_NAME "PostgreSQL"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "PostgreSQL 9.5.4"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "postgresql"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "9.5.4"

/* Define to the name of a signed 128-bit integer type. */
/* #undef PG_INT128_TYPE */

/* Define to the name of a signed 64-bit integer type. */
#define PG_INT64_TYPE long long int

/* Define to the name of the default PostgreSQL service principal in Kerberos
   (GSSAPI). (--with-krb-srvnam=NAME) */
#define PG_KRB_SRVNAM "postgres"

/* PostgreSQL major version as a string */
#define PG_MAJORVERSION "9.5"

/* Define to gnu_printf if compiler supports it, else printf. */
#define PG_PRINTF_ATTRIBUTE printf

/* Define to 1 if "static inline" works without unwanted warnings from
   compilations where static inline functions are defined but not called. */
#define PG_USE_INLINE 1

/* PostgreSQL version as a string */
#define PG_VERSION "9.5.4"

/* PostgreSQL version as a number */
#define PG_VERSION_NUM 90504

/* A string containing the version number, platform, and C compiler */
#define PG_VERSION_STR "PostgreSQL 9.5.4 on i686-pc-linux-gnu, compiled by gcc (GCC) 4.1.2 20080704 (Red Hat 4.1.2-55), 32-bit"

/* Define to 1 to allow profiling output to be saved separately for each
   process. */
/* #undef PROFILE_PID_DIR */

/* RELSEG_SIZE is the maximum number of blocks allowed in one disk file. Thus,
   the maximum size of a single file is RELSEG_SIZE * BLCKSZ; relations bigger
   than that are divided into multiple files. RELSEG_SIZE * BLCKSZ must be
   less than your OS' limit on file size. This is often 2 GB or 4GB in a
   32-bit operating system, unless you have large file support enabled. By
   default, we make the limit 1 GB to avoid any possible integer-overflow
   problems within the OS. A limit smaller than necessary only means we divide
   a large relation into more chunks than necessary, so it seems best to err
   in the direction of a small limit. A power-of-2 value is recommended to
   save a few cycles in md.c, but is not absolutely required. Changing
   RELSEG_SIZE requires an initdb. */
#define RELSEG_SIZE 131072

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 4

/* The size of `off_t', as computed by sizeof. */
#define SIZEOF_OFF_T 8

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 4

/* The size of `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 4

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to 1 if strerror_r() returns a int. */
/* #undef STRERROR_R_INT */

/* Define to 1 if your <sys/time.h> declares `struct tm'. */
/* #undef TM_IN_SYS_TIME */

/* Define to 1 to build with assertion checks. (--enable-cassert) */
/* #undef USE_ASSERT_CHECKING */

/* Define to 1 to build with Bonjour support. (--with-bonjour) */
/* #undef USE_BONJOUR */

/* Define to 1 if you want float4 values to be passed by value.
   (--enable-float4-byval) */
#define USE_FLOAT4_BYVAL 1

/* Define to 1 if you want float8, int8, etc values to be passed by value.
   (--enable-float8-byval) */
/* #undef USE_FLOAT8_BYVAL */

/* Define to 1 if you want 64-bit integer timestamp and interval support.
   (--enable-integer-datetimes) */
#define USE_INTEGER_DATETIMES 1

/* Define to 1 to build with LDAP support. (--with-ldap) */
//#define USE_LDAP 0

/* Define to 1 to build with XML support. (--with-libxml) */
#define USE_LIBXML 1

/* Define to 1 to use XSLT support when building contrib/xml2.
   (--with-libxslt) */
#define USE_LIBXSLT 1

/* Define to select named POSIX semaphores. */
/* #undef USE_NAMED_POSIX_SEMAPHORES */

/* Define to build with OpenSSL support. (--with-openssl) */
#define USE_OPENSSL 0

#define USE_OPENSSL_RANDOM 0

#define FRONTEND 1

/* Define to 1 to build with PAM support. (--with-pam) */
#define USE_PAM 1

/* Use replacement snprintf() functions. */
/* #undef USE_REPL_SNPRINTF */

/* Define to 1 to use Intel SSE 4.2 CRC instructions with a runtime check. */
#define USE_SLICING_BY_8_CRC32C 1

/* Define to 1 use Intel SSE 4.2 CRC instructions. */
/* #undef USE_SSE42_CRC32C */

/* Define to 1 to use Intel SSSE 4.2 CRC instructions with a runtime check. */
/* #undef USE_SSE42_CRC32C_WITH_RUNTIME_CHECK */

/* Define to select SysV-style semaphores. */
#define USE_SYSV_SEMAPHORES 1

/* Define to select SysV-style shared memory. */
#define USE_SYSV_SHARED_MEMORY 1

/* Define to select unnamed POSIX semaphores. */
/* #undef USE_UNNAMED_POSIX_SEMAPHORES */

/* Define to select Win32-style semaphores. */
/* #undef USE_WIN32_SEMAPHORES */

/* Define to select Win32-style shared memory. */
/* #undef USE_WIN32_SHARED_MEMORY */

/* Define to 1 to build with ZSTD support. (--with-zstd) */
/* #undef USE_ZSTD */

/* Define to 1 if `wcstombs_l' requires <xlocale.h>. */
/* #undef WCSTOMBS_L_IN_XLOCALE */

/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* Size of a WAL file block. This need have no particular relation to BLCKSZ.
   XLOG_BLCKSZ must be a power of 2, and if your system supports O_DIRECT I/O,
   XLOG_BLCKSZ must be a multiple of the alignment requirement for direct-I/O
   buffers, else direct I/O may fail. Changing XLOG_BLCKSZ requires an initdb.
   */
#define XLOG_BLCKSZ 8192

/* XLOG_SEG_SIZE is the size of a single WAL file. This must be a power of 2
   and larger than XLOG_BLCKSZ (preferably, a great deal larger than
   XLOG_BLCKSZ). Changing XLOG_SEG_SIZE requires an initdb. */
#define XLOG_SEG_SIZE (16 * 1024 * 1024)



/* Number of bits in a file offset, on hosts where this is settable. */
#define _FILE_OFFSET_BITS 64

/* Define to 1 to make fseeko visible on some hosts (e.g. glibc 2.2). */
/* #undef _LARGEFILE_SOURCE */

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif

/* Define to the type of a signed integer type wide enough to hold a pointer,
   if such a type exists, and if the system does not define it. */
/* #undef intptr_t */

/* Define to empty if the C compiler does not understand signed types. */
/* #undef signed */

/* Define to the type of an unsigned integer type wide enough to hold a
   pointer, if such a type exists, and if the system does not define it. */
/* #undef uintptr_t */
