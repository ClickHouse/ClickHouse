/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */


/* acconfig.h - autoheader configuration input */
/*
 * Copyright (c) 1998-2003 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact
 *      Office of Technology Transfer
 *      Carnegie Mellon University
 *      5000 Forbes Avenue
 *      Pittsburgh, PA  15213-3890
 *      (412) 268-4387, fax: (412) 268-7395
 *      tech-transfer@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef CONFIG_H
#define CONFIG_H


/* Include SASLdb Support */
/* #undef AUTH_SASLDB */

/* Do we need a leading _ for dlsym? */
/* #undef DLSYM_NEEDS_UNDERSCORE */

/* Should we build a shared plugin (via dlopen) library? */
#define DO_DLOPEN /**/

/* should we support sasl_checkapop? */
#define DO_SASL_CHECKAPOP /**/

/* should we support setpass() for SRP? */
/* #undef DO_SRP_SETPASS */

/* Define if your getpwnam_r()/getspnam_r() functions take 5 arguments */
#define GETXXNAM_R_5ARG 1

/* should we mutex-wrap calls into the GSS library? */
#define GSS_USE_MUTEXES /**/

/* Enable 'alwaystrue' password verifier? */
/* #undef HAVE_ALWAYSTRUE */

/* Define to 1 if you have the `asprintf' function. */
#define HAVE_ASPRINTF 1

/* Include support for Courier's authdaemond? */
#define HAVE_AUTHDAEMON /**/

/* Define to 1 if you have the <crypt.h> header file. */
#define HAVE_CRYPT_H 1

/* Define to 1 if you have the <des.h> header file. */
/* #undef HAVE_DES_H */

/* Define to 1 if you have the <dirent.h> header file, and it defines `DIR'.
   */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the `dns_lookup' function. */
/* #undef HAVE_DNS_LOOKUP */

/* Define to 1 if you have the `dn_expand' function. */
/* #undef HAVE_DN_EXPAND */

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Do we have a getaddrinfo? */
#define HAVE_GETADDRINFO /**/

/* Define to 1 if you have the `getdomainname' function. */
#define HAVE_GETDOMAINNAME 1

/* Define to 1 if you have the `gethostname' function. */
#define HAVE_GETHOSTNAME 1

/* Do we have a getnameinfo() function? */
#define HAVE_GETNAMEINFO /**/

/* Define to 1 if you have the `getpassphrase' function. */
/* #undef HAVE_GETPASSPHRASE */

/* Define to 1 if you have the `getpwnam' function. */
#define HAVE_GETPWNAM 1

/* Define to 1 if you have the `getspnam' function. */
#define HAVE_GETSPNAM 1

/* do we have getsubopt()? */
#define HAVE_GETSUBOPT /**/

/* Define to 1 if you have the `gettimeofday' function. */
#define HAVE_GETTIMEOFDAY 1

/* Include GSSAPI/Kerberos 5 Support */
#define HAVE_GSSAPI /**/

/* Define to 1 if you have the <gssapi/gssapi_ext.h> header file. */
#define HAVE_GSSAPI_GSSAPI_EXT_H 1

/* Define if you have the gssapi/gssapi.h header file */
/* #undef HAVE_GSSAPI_GSSAPI_H */

/* Define to 1 if you have the <gssapi/gssapi_krb5.h> header file. */
#define HAVE_GSSAPI_GSSAPI_KRB5_H 1

/* Define if you have the gssapi.h header file */
#define HAVE_GSSAPI_H /**/

/* Define if your GSSAPI implementation defines
   gsskrb5_register_acceptor_identity */
#define HAVE_GSSKRB5_REGISTER_ACCEPTOR_IDENTITY 1

/* Define if your GSSAPI implementation defines GSS_C_NT_HOSTBASED_SERVICE */
#define HAVE_GSS_C_NT_HOSTBASED_SERVICE /**/

/* Define if your GSSAPI implementation defines GSS_C_NT_USER_NAME */
#define HAVE_GSS_C_NT_USER_NAME /**/

/* Define if your GSSAPI implementation defines GSS_C_SEC_CONTEXT_SASL_SSF */
#define HAVE_GSS_C_SEC_CONTEXT_SASL_SSF /**/

/* Define to 1 if you have the `gss_decapsulate_token' function. */
#define HAVE_GSS_DECAPSULATE_TOKEN 1

/* Define to 1 if you have the `gss_encapsulate_token' function. */
#define HAVE_GSS_ENCAPSULATE_TOKEN 1

/* Define to 1 if you have the `gss_get_name_attribute' function. */
#define HAVE_GSS_GET_NAME_ATTRIBUTE 1

/* Define if your GSSAPI implementation defines gss_inquire_sec_context_by_oid
   */
#define HAVE_GSS_INQUIRE_SEC_CONTEXT_BY_OID 1

/* Define to 1 if you have the `gss_oid_equal' function. */
#define HAVE_GSS_OID_EQUAL 1

/* Define if your GSSAPI implementation supports SPNEGO */
#define HAVE_GSS_SPNEGO /**/

/* Include HTTP form Support */
/* #undef HAVE_HTTPFORM */

/* Define to 1 if you have the `inet_aton' function. */
#define HAVE_INET_ATON 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `jrand48' function. */
#define HAVE_JRAND48 1

/* Do we have Kerberos 4 Support? */
/* #undef HAVE_KRB */

/* Define to 1 if you have the <krb5.h> header file. */
#define HAVE_KRB5_H 1

/* Define to 1 if you have the `krb_get_err_text' function. */
/* #undef HAVE_KRB_GET_ERR_TEXT */

/* Define to 1 if you have the <lber.h> header file. */
/* #undef HAVE_LBER_H */

/* Support for LDAP? */
/* #undef HAVE_LDAP */

/* Define to 1 if you have the <ldap.h> header file. */
/* #undef HAVE_LDAP_H */

/* Define to 1 if you have the `resolv' library (-lresolv). */
#define HAVE_LIBRESOLV 1

/* Define to 1 if you have the <limits.h> header file. */
#define HAVE_LIMITS_H 1

/* Define to 1 if you have the <malloc.h> header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the `memcpy' function. */
#define HAVE_MEMCPY 1

/* Define to 1 if you have the `memmem' function. */
#define HAVE_MEMMEM 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mkdir' function. */
#define HAVE_MKDIR 1

/* Do we have mysql support? */
/* #undef HAVE_MYSQL */

/* Define to 1 if you have the <ndir.h> header file, and it defines `DIR'. */
/* #undef HAVE_NDIR_H */

/* Do we have OpenSSL? */
#define HAVE_OPENSSL /**/

/* Use OPIE for server-side OTP? */
/* #undef HAVE_OPIE */

/* Support for PAM? */
/* #undef HAVE_PAM */

/* Define to 1 if you have the <paths.h> header file. */
#define HAVE_PATHS_H 1

/* Do we have Postgres support? */
/* #undef HAVE_PGSQL */

/* Include Support for pwcheck daemon? */
/* #undef HAVE_PWCHECK */

/* Include support for saslauthd? */
#define HAVE_SASLAUTHD /**/

/* Define to 1 if you have the `select' function. */
#define HAVE_SELECT 1

/* Do we have SHA512? */
#define HAVE_SHA512 /**/

/* Include SIA Support */
/* #undef HAVE_SIA */

/* Does the system have snprintf()? */
#define HAVE_SNPRINTF /**/

/* Does sockaddr have an sa_len? */
/* #undef HAVE_SOCKADDR_SA_LEN */

/* Define to 1 if you have the `socket' function. */
#define HAVE_SOCKET 1

/* Do we have a socklen_t? */
#define HAVE_SOCKLEN_T /**/

/* Do we have SQLite support? */
/* #undef HAVE_SQLITE */

/* Do we have SQLite3 support? */
/* #undef HAVE_SQLITE3 */

/* Is there an ss_family in sockaddr_storage? */
#define HAVE_SS_FAMILY /**/

/* Define to 1 if you have the <stdarg.h> header file. */
#define HAVE_STDARG_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strchr' function. */
#define HAVE_STRCHR 1

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the `strerror' function. */
#define HAVE_STRERROR 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strlcat' function. */
/* #undef HAVE_STRLCAT */

/* Define to 1 if you have the `strlcpy' function. */
/* #undef HAVE_STRLCPY */

/* Define to 1 if you have the `strspn' function. */
#define HAVE_STRSPN 1

/* Define to 1 if you have the `strstr' function. */
#define HAVE_STRSTR 1

/* Define to 1 if you have the `strtol' function. */
#define HAVE_STRTOL 1

/* Do we have struct sockaddr_stroage? */
#define HAVE_STRUCT_SOCKADDR_STORAGE /**/

/* Define to 1 if you have the <sysexits.h> header file. */
#define HAVE_SYSEXITS_H 1

/* Define to 1 if you have the `syslog' function. */
#define HAVE_SYSLOG 1

/* Define to 1 if you have the <syslog.h> header file. */
#define HAVE_SYSLOG_H 1

/* Define to 1 if you have the <sys/dir.h> header file, and it defines `DIR'.
   */
/* #undef HAVE_SYS_DIR_H */

/* Define to 1 if you have the <sys/file.h> header file. */
#define HAVE_SYS_FILE_H 1

/* Define to 1 if you have the <sys/ndir.h> header file, and it defines `DIR'.
   */
/* #undef HAVE_SYS_NDIR_H */

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
#define HAVE_SYS_UIO_H 1

/* Define to 1 if you have <sys/wait.h> that is POSIX.1 compatible. */
#define HAVE_SYS_WAIT_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the <varargs.h> header file. */
/* #undef HAVE_VARARGS_H */

/* Does the system have vsnprintf()? */
#define HAVE_VSNPRINTF /**/

/* Define to 1 if you have the <ws2tcpip.h> header file. */
/* #undef HAVE_WS2TCPIP_H */

/* Should we keep handle to DB open in SASLDB plugin? */
/* #undef KEEP_DB_OPEN */

/* Ignore IP Address in Kerberos 4 tickets? */
/* #undef KRB4_IGNORE_IP_ADDRESS */

/* Using Heimdal */
/* #undef KRB5_HEIMDAL */

/* Define to the sub-directory where libtool stores uninstalled libraries. */
#define LT_OBJDIR ".libs/"

/* Name of package */
#define PACKAGE "cyrus-sasl"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "https://github.com/cyrusimap/cyrus-sasl/issues"

/* Define to the full name of this package. */
#define PACKAGE_NAME "cyrus-sasl"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "cyrus-sasl 2.1.27"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "cyrus-sasl"

/* Define to the home page for this package. */
#define PACKAGE_URL "http://www.cyrusimap.org"

/* Define to the version of this package. */
#define PACKAGE_VERSION "2.1.27"

/* Where do we look for Courier authdaemond's socket? */
#define PATH_AUTHDAEMON_SOCKET "/dev/null"

/* Where do we look for saslauthd's socket? */
#define PATH_SASLAUTHD_RUNDIR "/var/state/saslauthd"

/* Force a preferred mechanism */
/* #undef PREFER_MECH */

/* Location of pwcheck socket */
/* #undef PWCHECKDIR */

/* Define as the return type of signal handlers (`int' or `void'). */
#define RETSIGTYPE void

/* Use BerkeleyDB for SASLdb */
/* #undef SASL_BERKELEYDB */

/* Path to default SASLdb database */
#define SASL_DB_PATH "/etc/sasldb2"

/* File to use for source of randomness */
#define SASL_DEV_RANDOM "/dev/urandom"

/* Use GDBM for SASLdb */
/* #undef SASL_GDBM */

/* Use LMDB for SASLdb */
/* #undef SASL_LMDB */

/* Use NDBM for SASLdb */
/* #undef SASL_NDBM */

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* Link ANONYMOUS Statically */
// #define STATIC_ANONYMOUS /**/

/* Link CRAM-MD5 Statically */
// #define STATIC_CRAMMD5 /**/

/* Link DIGEST-MD5 Statically */
// #define STATIC_DIGESTMD5 /**/

/* Link GSSAPI Statically */
#define STATIC_GSSAPIV2 /**/

/* User KERBEROS_V4 Staticly */
/* #undef STATIC_KERBEROS4 */

/* Link ldapdb plugin Statically */
/* #undef STATIC_LDAPDB */

/* Link LOGIN Statically */
/* #undef STATIC_LOGIN */

/* Link NTLM Statically */
/* #undef STATIC_NTLM */

/* Link OTP Statically */
// #define STATIC_OTP /**/

/* Link PASSDSS Statically */
/* #undef STATIC_PASSDSS */

/* Link PLAIN Staticly */
// #define STATIC_PLAIN /**/

/* Link SASLdb Staticly */
// #define STATIC_SASLDB /**/

/* Link SCRAM Statically */
// #define STATIC_SCRAM /**/

/* Link SQL plugin statically */
/* #undef STATIC_SQL */

/* Link SRP Statically */
/* #undef STATIC_SRP */

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
#define TIME_WITH_SYS_TIME 1

/* Should we try to dlopen() plugins while statically compiled? */
/* #undef TRY_DLOPEN_WHEN_STATIC */

/* use the doors IPC API for saslauthd? */
/* #undef USE_DOORS */

/* Enable extensions on AIX 3, Interix.  */
#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
/* Enable threading extensions on Solaris.  */
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif
/* Enable extensions on HP NonStop.  */
#ifndef _TANDEM_SOURCE
# define _TANDEM_SOURCE 1
#endif
/* Enable general extensions on Solaris.  */
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif


/* Version number of package */
#define VERSION "2.1.27"

/* Use DES */
#define WITH_DES /**/

/* Linking against dmalloc? */
/* #undef WITH_DMALLOC */

/* Use RC4 */
#define WITH_RC4 /**/

/* Use OpenSSL DES Implementation */
#define WITH_SSL_DES /**/

/* Define to 1 if on MINIX. */
/* #undef _MINIX */

/* Define to 2 if the system does not provide POSIX.1 features except with
   this defined. */
/* #undef _POSIX_1_SOURCE */

/* Define to 1 if you need to in order for `stat' and other things to work. */
/* #undef _POSIX_SOURCE */

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif

/* Define to `int' if <sys/types.h> does not define. */
/* #undef mode_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef pid_t */




/* Create a struct iovec if we need one */
#if !defined(_WIN32)
#if !defined(HAVE_SYS_UIO_H)
/* (win32 is handled in sasl.h) */
struct iovec {
    char *iov_base;
    long iov_len;
};
#else
#include <sys/types.h>
#include <sys/uio.h>
#endif
#endif

/* location of the random number generator */
#ifdef DEV_RANDOM
/* #undef DEV_RANDOM */
#endif
#define DEV_RANDOM SASL_DEV_RANDOM

/* if we've got krb_get_err_txt, we might as well use it;
   especially since krb_err_txt isn't in some newer distributions
   (MIT Kerb for Mac 4 being a notable example). If we don't have
   it, we fall back to the krb_err_txt array */
#ifdef HAVE_KRB_GET_ERR_TEXT
#define get_krb_err_txt krb_get_err_text
#else
#define get_krb_err_txt(X) (krb_err_txt[(X)])
#endif

/* Make Solaris happy... */
#ifndef __EXTENSIONS__
#define __EXTENSIONS__ 1
#endif

/* Make Linux happy... */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#define SASL_PATH_ENV_VAR "SASL_PATH"
#define SASL_CONF_PATH_ENV_VAR "SASL_CONF_PATH"

#include <stdlib.h>
#include <sys/types.h>
#ifndef WIN32
# include <sys/socket.h>
# include <netdb.h>
# include <netinet/in.h>
# ifdef HAVE_SYS_PARAM_H
#  include <sys/param.h>
# endif
#else /* WIN32 */
# include <winsock2.h>
#endif /* WIN32 */
#include <string.h>

#ifndef HAVE_SOCKLEN_T
typedef unsigned int socklen_t;
#endif /* HAVE_SOCKLEN_T */

#if !defined(HAVE_STRUCT_SOCKADDR_STORAGE) && !defined(WIN32)
#define	_SS_MAXSIZE	128	/* Implementation specific max size */
#define	_SS_PADSIZE	(_SS_MAXSIZE - sizeof (struct sockaddr))

struct sockaddr_storage {
	struct	sockaddr ss_sa;
	char		__ss_pad2[_SS_PADSIZE];
};
# define ss_family ss_sa.sa_family
#endif /* !HAVE_STRUCT_SOCKADDR_STORAGE */

#ifndef AF_INET6
/* Define it to something that should never appear */
#define	AF_INET6	AF_MAX
#endif

#ifndef HAVE_GETADDRINFO
#define	getaddrinfo	sasl_getaddrinfo
#define	freeaddrinfo	sasl_freeaddrinfo
#define	gai_strerror	sasl_gai_strerror
#endif

#ifndef HAVE_GETNAMEINFO
#define	getnameinfo	sasl_getnameinfo
#endif

#if !defined(HAVE_GETNAMEINFO) || !defined(HAVE_GETADDRINFO)
#include "gai.h"
#endif

#ifndef AI_NUMERICHOST   /* support glibc 2.0.x */
#define AI_NUMERICHOST  4
#define NI_NUMERICHOST  2
#define NI_NAMEREQD     4
#define NI_NUMERICSERV  8
#endif

#ifndef HAVE_SYSEXITS_H
#include "exits.h"
#else
#include "sysexits.h"
#endif

/* Get the correct time.h */
#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

#ifndef HIER_DELIMITER
#define HIER_DELIMITER '/'
#endif

#ifdef WIN32
#define SASL_ROOT_KEY "SOFTWARE\\Carnegie Mellon\\Project Cyrus\\SASL Library"
#define SASL_PLUGIN_PATH_ATTR "SearchPath"
#define SASL_CONF_PATH_ATTR "ConfFile"

#include <windows.h>
inline static unsigned int sleep(unsigned int seconds) {
       Sleep(seconds * 1000);
       return 0;
}
#endif

/* handy string manipulation functions */
#ifndef HAVE_STRLCPY
extern size_t saslauthd_strlcpy(char *dst, const char *src, size_t len);
#define strlcpy(x,y,z) saslauthd_strlcpy((x),(y),(z))
#endif
#ifndef HAVE_STRLCAT
extern size_t saslauthd_strlcat(char *dst, const char *src, size_t len);
#define strlcat(x,y,z) saslauthd_strlcat((x),(y),(z))
#endif
#ifndef HAVE_ASPRINTF
extern int asprintf(char **str, const char *fmt, ...);
#endif

#endif /* CONFIG_H */


#if defined __GNUC__ &&  __GNUC__ > 6
    #define GCC_FALLTHROUGH __attribute__((fallthrough));
#else
    #define GCC_FALLTHROUGH /* fall through */
#endif
