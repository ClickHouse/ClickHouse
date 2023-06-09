/* include/autoconf.h.  Generated from autoconf.h.in by configure.  */
/* include/autoconf.h.in.  Generated from configure.in by autoheader.  */


#ifndef KRB5_AUTOCONF_H
#define KRB5_AUTOCONF_H


/* Define if AES-NI support is enabled */
/* #undef AESNI */

/* Define if socket can't be bound to 0.0.0.0 */
/* #undef BROKEN_STREAMS_SOCKETS */

/* Define if va_list objects can be simply copied by assignment. */
/* #undef CAN_COPY_VA_LIST */

/* Define to reduce code size even if it means more cpu usage */
/* #undef CONFIG_SMALL */

/* Define if __attribute__((constructor)) works */
#define CONSTRUCTOR_ATTR_WORKS 1

/* Define to default ccache name */
#define DEFCCNAME "FILE:/tmp/krb5cc_%{uid}"

/* Define to default client keytab name */
#define DEFCKTNAME "FILE:/etc/krb5/user/%{euid}/client.keytab"

/* Define to default keytab name */
#define DEFKTNAME "FILE:/etc/krb5.keytab"

/* Define if library initialization should be delayed until first use */
#define DELAY_INITIALIZER 1

/* Define if __attribute__((destructor)) works */
#define DESTRUCTOR_ATTR_WORKS 1

/* Define to disable PKINIT plugin support */
/* #undef DISABLE_PKINIT */

/* Define if LDAP KDB support within the Kerberos library (mainly ASN.1 code)
   should be enabled. */
/* #undef ENABLE_LDAP */

/* Define if translation functions should be used. */
#define ENABLE_NLS 1

/* Define if thread support enabled */
#define ENABLE_THREADS 1

/* Define as return type of endrpcent */
#define ENDRPCENT_TYPE void

/* Define if Fortuna PRNG is selected */
#define FORTUNA 1

/* Define to the type of elements in the array set by `getgroups'. Usually
   this is either `int' or `gid_t'. */
#define GETGROUPS_T gid_t

/* Define if gethostbyname_r returns int rather than struct hostent * */
#define GETHOSTBYNAME_R_RETURNS_INT 1

/* Type of getpeername second argument. */
#define GETPEERNAME_ARG3_TYPE GETSOCKNAME_ARG3_TYPE

/* Define if getpwnam_r exists but takes only 4 arguments (e.g., POSIX draft 6
   implementations like some Solaris releases). */
/* #undef GETPWNAM_R_4_ARGS */

/* Define if getpwnam_r returns an int */
#define GETPWNAM_R_RETURNS_INT 1

/* Define if getpwuid_r exists but takes only 4 arguments (e.g., POSIX draft 6
   implementations like some Solaris releases). */
/* #undef GETPWUID_R_4_ARGS */

/* Define if getservbyname_r returns int rather than struct servent * */
#define GETSERVBYNAME_R_RETURNS_INT 1

/* Type of pointer target for argument 3 to getsockname */
#define GETSOCKNAME_ARG3_TYPE socklen_t

/* Define if gmtime_r returns int instead of struct tm pointer, as on old
   HP-UX systems. */
/* #undef GMTIME_R_RETURNS_INT */

/* Define if va_copy macro or function is available. */
#define HAS_VA_COPY 1

/* Define to 1 if you have the `access' function. */
#define HAVE_ACCESS 1

/* Define to 1 if you have the <alloca.h> header file. */
#define HAVE_ALLOCA_H 1

/* Define to 1 if you have the <arpa/inet.h> header file. */
#define HAVE_ARPA_INET_H 1

/* Define to 1 if you have the `bswap16' function. */
/* #undef HAVE_BSWAP16 */

/* Define to 1 if you have the `bswap64' function. */
/* #undef HAVE_BSWAP64 */

/* Define to 1 if bswap_16 is available via byteswap.h */
#define HAVE_BSWAP_16 1

/* Define to 1 if bswap_64 is available via byteswap.h */
#define HAVE_BSWAP_64 1

/* Define if bt_rseq is available, for recursive btree traversal. */
#define HAVE_BT_RSEQ 1

/* Define to 1 if you have the <byteswap.h> header file. */
#define HAVE_BYTESWAP_H 1

/* Define to 1 if you have the `chmod' function. */
#define HAVE_CHMOD 1

/* Define if cmocka library is available. */
/* #undef HAVE_CMOCKA */

/* Define to 1 if you have the `compile' function. */
/* #undef HAVE_COMPILE */

/* Define if com_err has compatible gettext support */
#define HAVE_COM_ERR_INTL 1

/* Define to 1 if you have the <cpuid.h> header file. */
/* #undef HAVE_CPUID_H */

/* Define to 1 if you have the `daemon' function. */
#define HAVE_DAEMON 1

/* Define to 1 if you have the declaration of `strerror_r', and to 0 if you
   don't. */
#define HAVE_DECL_STRERROR_R 1

/* Define to 1 if you have the <dirent.h> header file, and it defines `DIR'.
   */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the `dn_skipname' function. */
#define HAVE_DN_SKIPNAME 0

/* Define to 1 if you have the <endian.h> header file. */
#define HAVE_ENDIAN_H 1

/* Define to 1 if you have the <errno.h> header file. */
#define HAVE_ERRNO_H 1

/* Define to 1 if you have the `fchmod' function. */
#define HAVE_FCHMOD 1

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the `flock' function. */
#define HAVE_FLOCK 1

/* Define to 1 if you have the `fnmatch' function. */
#define HAVE_FNMATCH 1

/* Define to 1 if you have the <fnmatch.h> header file. */
#define HAVE_FNMATCH_H 1

/* Define if you have the getaddrinfo function */
#define HAVE_GETADDRINFO 1

/* Define to 1 if you have the `getcwd' function. */
#define HAVE_GETCWD 1

/* Define to 1 if you have the `getenv' function. */
#define HAVE_GETENV 1

/* Define to 1 if you have the `geteuid' function. */
#define HAVE_GETEUID 1

/* Define if gethostbyname_r exists and its return type is known */
#define HAVE_GETHOSTBYNAME_R 1

/* Define to 1 if you have the `getnameinfo' function. */
#define HAVE_GETNAMEINFO 1

/* Define if system getopt should be used. */
#define HAVE_GETOPT 1

/* Define if system getopt_long should be used. */
#define HAVE_GETOPT_LONG 1

/* Define if getpwnam_r is available and useful. */
#define HAVE_GETPWNAM_R 1

/* Define if getpwuid_r is available and useful. */
#define HAVE_GETPWUID_R 1

/* Define if getservbyname_r exists and its return type is known */
#define HAVE_GETSERVBYNAME_R 1

/* Have the gettimeofday function */
#define HAVE_GETTIMEOFDAY 1

/* Define to 1 if you have the `getusershell' function. */
#define HAVE_GETUSERSHELL 1

/* Define to 1 if you have the `gmtime_r' function. */
#define HAVE_GMTIME_R 1

/* Define to 1 if you have the <ifaddrs.h> header file. */
#define HAVE_IFADDRS_H 1

/* Define to 1 if you have the `inet_ntop' function. */
#define HAVE_INET_NTOP 1

/* Define to 1 if you have the `inet_pton' function. */
#define HAVE_INET_PTON 1

/* Define to 1 if the system has the type `int16_t'. */
#define HAVE_INT16_T 1

/* Define to 1 if the system has the type `int32_t'. */
#define HAVE_INT32_T 1

/* Define to 1 if the system has the type `int8_t'. */
#define HAVE_INT8_T 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the <keyutils.h> header file. */
/* #undef HAVE_KEYUTILS_H */

/* Define to 1 if you have the <lber.h> header file. */
/* #undef HAVE_LBER_H */

/* Define to 1 if you have the <ldap.h> header file. */
/* #undef HAVE_LDAP_H */

/* Define to 1 if you have the `crypto' library (-lcrypto). */
#define HAVE_LIBCRYPTO 1

/* Define if building with libedit. */
/* #undef HAVE_LIBEDIT */

/* Define to 1 if you have the `nsl' library (-lnsl). */
/* #undef HAVE_LIBNSL */

/* Define to 1 if you have the `resolv' library (-lresolv). */
#define HAVE_LIBRESOLV 1

/* Define to 1 if you have the `socket' library (-lsocket). */
/* #undef HAVE_LIBSOCKET */

/* Define if the util library is available */
#define HAVE_LIBUTIL 1

/* Define to 1 if you have the <limits.h> header file. */
#define HAVE_LIMITS_H 1

/* Define to 1 if you have the `localtime_r' function. */
#define HAVE_LOCALTIME_R 1

/* Define to 1 if you have the <machine/byte_order.h> header file. */
/* #undef HAVE_MACHINE_BYTE_ORDER_H */

/* Define to 1 if you have the <machine/endian.h> header file. */
/* #undef HAVE_MACHINE_ENDIAN_H */

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mkstemp' function. */
#define HAVE_MKSTEMP 1

/* Define to 1 if you have the <ndir.h> header file, and it defines `DIR'. */
/* #undef HAVE_NDIR_H */

/* Define to 1 if you have the <netdb.h> header file. */
#define HAVE_NETDB_H 1

/* Define if netdb.h declares h_errno */
#define HAVE_NETDB_H_H_ERRNO 1

/* Define to 1 if you have the <netinet/in.h> header file. */
#define HAVE_NETINET_IN_H 1

/* Define to 1 if you have the `ns_initparse' function. */
#define HAVE_NS_INITPARSE 0

/* Define to 1 if you have the `ns_name_uncompress' function. */
#define HAVE_NS_NAME_UNCOMPRESS 0

/* Define if OpenSSL supports cms. */
#define HAVE_OPENSSL_CMS 1

/* Define to 1 if you have the <paths.h> header file. */
#define HAVE_PATHS_H 1

/* Define if persistent keyrings are supported */
/* #undef HAVE_PERSISTENT_KEYRING */

/* Define to 1 if you have the <poll.h> header file. */
#define HAVE_POLL_H 1

/* Define if #pragma weak references work */
#define HAVE_PRAGMA_WEAK_REF 1

/* Define if you have POSIX threads libraries and header files. */
#define HAVE_PTHREAD 1

/* Define to 1 if you have the `pthread_once' function. */
/* #undef HAVE_PTHREAD_ONCE */

/* Have PTHREAD_PRIO_INHERIT. */
#define HAVE_PTHREAD_PRIO_INHERIT 1

/* Define to 1 if you have the `pthread_rwlock_init' function. */
/* #undef HAVE_PTHREAD_RWLOCK_INIT */

/* Define if pthread_rwlock_init is provided in the thread library. */
#define HAVE_PTHREAD_RWLOCK_INIT_IN_THREAD_LIB 1

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define if building with GNU Readline. */
/* #undef HAVE_READLINE */

/* Define if regcomp exists and functions */
#define HAVE_REGCOMP 1

/* Define to 1 if you have the `regexec' function. */
#define HAVE_REGEXEC 1

/* Define to 1 if you have the <regexpr.h> header file. */
/* #undef HAVE_REGEXPR_H */

/* Define to 1 if you have the <regex.h> header file. */
#define HAVE_REGEX_H 1

/* Define to 1 if you have the `res_nclose' function. */
#define HAVE_RES_NCLOSE 1

/* Define to 1 if you have the `res_ndestroy' function. */
/* #undef HAVE_RES_NDESTROY */

/* Define to 1 if you have the `res_ninit' function. */
#define HAVE_RES_NINIT 1

/* Define to 1 if you have the `res_nsearch' function. */
#define HAVE_RES_NSEARCH 0

/* Define to 1 if you have the `res_search' function */
#define HAVE_RES_SEARCH 1

/* Define to 1 if you have the `re_comp' function. */
#define HAVE_RE_COMP 1

/* Define to 1 if you have the `re_exec' function. */
#define HAVE_RE_EXEC 1

/* Define to 1 if you have the <sasl/sasl.h> header file. */
/* #undef HAVE_SASL_SASL_H */

/* Define if struct sockaddr contains sa_len */
/* #undef HAVE_SA_LEN */

/* Define to 1 if you have the `setegid' function. */
#define HAVE_SETEGID 1

/* Define to 1 if you have the `setenv' function. */
#define HAVE_SETENV 1

/* Define to 1 if you have the `seteuid' function. */
#define HAVE_SETEUID 1

/* Define if setluid provided in OSF/1 security library */
/* #undef HAVE_SETLUID */

/* Define to 1 if you have the `setregid' function. */
#define HAVE_SETREGID 1

/* Define to 1 if you have the `setresgid' function. */
#define HAVE_SETRESGID 1

/* Define to 1 if you have the `setresuid' function. */
#define HAVE_SETRESUID 1

/* Define to 1 if you have the `setreuid' function. */
#define HAVE_SETREUID 1

/* Define to 1 if you have the `setsid' function. */
#define HAVE_SETSID 1

/* Define to 1 if you have the `setvbuf' function. */
#define HAVE_SETVBUF 1

/* Define if there is a socklen_t type. If not, probably use size_t */
#define HAVE_SOCKLEN_T 1

/* Define to 1 if you have the `srand' function. */
#define HAVE_SRAND 1

/* Define to 1 if you have the `srand48' function. */
#define HAVE_SRAND48 1

/* Define to 1 if you have the `srandom' function. */
#define HAVE_SRANDOM 1

/* Define to 1 if the system has the type `ssize_t'. */
#define HAVE_SSIZE_T 1

/* Define to 1 if you have the `stat' function. */
#define HAVE_STAT 1

/* Define to 1 if you have the <stddef.h> header file. */
#define HAVE_STDDEF_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `step' function. */
/* #undef HAVE_STEP */

/* Define to 1 if you have the `strchr' function. */
#define HAVE_STRCHR 1

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the `strerror' function. */
#define HAVE_STRERROR 1

/* Define to 1 if you have the `strerror_r' function. */
#define HAVE_STRERROR_R 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strlcpy' function. */
/* #undef HAVE_STRLCPY */

/* Define to 1 if you have the `strptime' function. */
#define HAVE_STRPTIME 1

/* Define to 1 if the system has the type `struct cmsghdr'. */
#define HAVE_STRUCT_CMSGHDR 1

/* Define if there is a struct if_laddrconf. */
/* #undef HAVE_STRUCT_IF_LADDRCONF */

/* Define to 1 if the system has the type `struct in6_pktinfo'. */
#define HAVE_STRUCT_IN6_PKTINFO 1

/* Define to 1 if the system has the type `struct in_pktinfo'. */
#define HAVE_STRUCT_IN_PKTINFO 1

/* Define if there is a struct lifconf. */
/* #undef HAVE_STRUCT_LIFCONF */

/* Define to 1 if the system has the type `struct rt_msghdr'. */
/* #undef HAVE_STRUCT_RT_MSGHDR */

/* Define to 1 if the system has the type `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE 1

/* Define to 1 if `st_mtimensec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_MTIMENSEC */

/* Define to 1 if `st_mtimespec.tv_nsec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC */

/* Define to 1 if `st_mtim.tv_nsec' is a member of `struct stat'. */
#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1

/* Define to 1 if you have the <sys/bswap.h> header file. */
/* #undef HAVE_SYS_BSWAP_H */

/* Define to 1 if you have the <sys/dir.h> header file, and it defines `DIR'.
   */
/* #undef HAVE_SYS_DIR_H */

/* Define if sys_errlist in libc */
#define HAVE_SYS_ERRLIST 1

/* Define to 1 if you have the <sys/file.h> header file. */
#define HAVE_SYS_FILE_H 1

/* Define to 1 if you have the <sys/filio.h> header file. */
/* #undef HAVE_SYS_FILIO_H */

/* Define to 1 if you have the <sys/ndir.h> header file, and it defines `DIR'.
   */
/* #undef HAVE_SYS_NDIR_H */

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/select.h> header file. */
#define HAVE_SYS_SELECT_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/sockio.h> header file. */
/* #undef HAVE_SYS_SOCKIO_H */

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
#define HAVE_SYS_UIO_H 1

/* Define if tcl.h found */
/* #undef HAVE_TCL_H */

/* Define if tcl/tcl.h found */
/* #undef HAVE_TCL_TCL_H */

/* Define to 1 if you have the `timegm' function. */
#define HAVE_TIMEGM 1

/* Define to 1 if you have the <time.h> header file. */
#define HAVE_TIME_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the `unsetenv' function. */
#define HAVE_UNSETENV 1

/* Define to 1 if the system has the type `u_char'. */
#define HAVE_U_CHAR 1

/* Define to 1 if the system has the type `u_int'. */
#define HAVE_U_INT 1

/* Define to 1 if the system has the type `u_int16_t'. */
#define HAVE_U_INT16_T 1

/* Define to 1 if the system has the type `u_int32_t'. */
#define HAVE_U_INT32_T 1

/* Define to 1 if the system has the type `u_int8_t'. */
#define HAVE_U_INT8_T 1

/* Define to 1 if the system has the type `u_long'. */
#define HAVE_U_LONG 1

/* Define to 1 if you have the `vasprintf' function. */
#define HAVE_VASPRINTF 1

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1

/* Define to 1 if you have the `vsprintf' function. */
#define HAVE_VSPRINTF 1

/* Define to 1 if the system has the type `__int128_t'. */
#define HAVE___INT128_T 1

/* Define to 1 if the system has the type `__uint128_t'. */
#define HAVE___UINT128_T 1

/* Define if errno.h declares perror */
/* #undef HDR_HAS_PERROR */

/* May need to be defined to enable IPv6 support, for example on IRIX */
/* #undef INET6 */

/* Define if MIT Project Athena default configuration should be used */
/* #undef KRB5_ATHENA_COMPAT */

/* Define for DNS support of locating realms and KDCs */
#undef KRB5_DNS_LOOKUP

/* Define to enable DNS lookups of Kerberos realm names */
/* #undef KRB5_DNS_LOOKUP_REALM */

/* Define if the KDC should return only vague error codes to clients */
/* #undef KRBCONF_VAGUE_ERRORS */

/* define if the system header files are missing prototype for daemon() */
/* #undef NEED_DAEMON_PROTO */

/* Define if in6addr_any is not defined in libc */
/* #undef NEED_INSIXADDR_ANY */

/* define if the system header files are missing prototype for
   ss_execute_command() */
/* #undef NEED_SS_EXECUTE_COMMAND_PROTO */

/* define if the system header files are missing prototype for strptime() */
/* #undef NEED_STRPTIME_PROTO */

/* define if the system header files are missing prototype for swab() */
/* #undef NEED_SWAB_PROTO */

/* Define if need to declare sys_errlist */
/* #undef NEED_SYS_ERRLIST */

/* define if the system header files are missing prototype for vasprintf() */
/* #undef NEED_VASPRINTF_PROTO */

/* Define if the KDC should use no lookaside cache */
/* #undef NOCACHE */

/* Define if references to pthread routines should be non-weak. */
/* #undef NO_WEAK_PTHREADS */

/* Define if lex produes code with yylineno */
/* #undef NO_YYLINENO */

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "krb5-bugs@mit.edu"

/* Define to the full name of this package. */
#define PACKAGE_NAME "Kerberos 5"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "Kerberos 5 1.17.1"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "krb5"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "1.17.1"

/* Define if setjmp indicates POSIX interface */
/* #undef POSIX_SETJMP */

/* Define if POSIX signal handling is used */
#define POSIX_SIGNALS 1

/* Define if POSIX signal handlers are used */
#define POSIX_SIGTYPE 1

/* Define if termios.h exists and tcsetattr exists */
#define POSIX_TERMIOS 1

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* Define as the return type of signal handlers (`int' or `void'). */
#define RETSIGTYPE void

/* Define as return type of setrpcent */
#define SETRPCENT_TYPE void

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8

/* The size of `time_t', as computed by sizeof. */
#define SIZEOF_TIME_T 8

/* Define to use OpenSSL for SPAKE preauth */
#define SPAKE_OPENSSL 1

/* Define for static plugin linkage */
/* #undef STATIC_PLUGINS */

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to 1 if strerror_r returns char *. */
#define STRERROR_R_CHAR_P 1

/* Define if sys_errlist is defined in errno.h */
#define SYS_ERRLIST_DECLARED 1

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
#define TIME_WITH_SYS_TIME 1

/* Define if no TLS implementation is selected */
/* #undef TLS_IMPL_NONE */

/* Define if TLS implementation is OpenSSL */
#define TLS_IMPL_OPENSSL 1

/* Define if you have dirent.h functionality */
#define USE_DIRENT_H 1

/* Define if dlopen should be used */
#define USE_DLOPEN 1

/* Define if the keyring ccache should be enabled */
/* #undef USE_KEYRING_CCACHE */

/* Define if link-time options for library finalization will be used */
/* #undef USE_LINKER_FINI_OPTION */

/* Define if link-time options for library initialization will be used */
/* #undef USE_LINKER_INIT_OPTION */

/* Define if sigprocmask should be used */
#define USE_SIGPROCMASK 1

/* Define if wait takes int as a argument */
#define WAIT_USES_INT 1

/* Define to 1 if `lex' declares `yytext' as a `char *' by default, not a
   `char[]'. */
#define YYTEXT_POINTER 1

/* Define to enable extensions in glibc */
#define _GNU_SOURCE 1

/* Define to enable C11 extensions */
#define __STDC_WANT_LIB_EXT1__ 1

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to `int' if <sys/types.h> doesn't define. */
/* #undef gid_t */

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif

/* Define krb5_sigtype to type of signal handler */
#define krb5_sigtype void

/* Define to `int' if <sys/types.h> does not define. */
/* #undef mode_t */

/* Define to `long int' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `long' if <sys/types.h> does not define. */
/* #undef time_t */

/* Define to `int' if <sys/types.h> doesn't define. */
/* #undef uid_t */


#if defined(__GNUC__) && !defined(inline)
/* Silence gcc pedantic warnings about ANSI C.  */
# define inline __inline__
#endif
#endif /* KRB5_AUTOCONF_H */
