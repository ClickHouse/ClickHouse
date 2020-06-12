/*
 * Public domain
 * string.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_STRING_H
#define LIBCRYPTOCOMPAT_STRING_H

#ifdef _MSC_VER
#if _MSC_VER >= 1900
#include <../ucrt/string.h>
#else
#include <../include/string.h>
#endif
#else
#include_next <string.h>
#endif

#include <sys/types.h>

#if defined(__sun) || defined(_AIX) || defined(__hpux)
/* Some functions historically defined in string.h were placed in strings.h by
 * SUS. Use the same hack as OS X and FreeBSD use to work around on AIX,
 * Solaris, and HPUX.
 */
#include <strings.h>
#endif

#ifndef HAVE_STRCASECMP
int strcasecmp(const char *s1, const char *s2);
int strncasecmp(const char *s1, const char *s2, size_t len);
#endif

#ifndef HAVE_STRLCPY
size_t strlcpy(char *dst, const char *src, size_t siz);
#endif

#ifndef HAVE_STRLCAT
size_t strlcat(char *dst, const char *src, size_t siz);
#endif

#ifndef HAVE_STRNDUP
char * strndup(const char *str, size_t maxlen);
/* the only user of strnlen is strndup, so only build it if needed */
#ifndef HAVE_STRNLEN
size_t strnlen(const char *str, size_t maxlen);
#endif
#endif

#ifndef HAVE_STRSEP
char *strsep(char **stringp, const char *delim);
#endif

#ifndef HAVE_EXPLICIT_BZERO
void explicit_bzero(void *, size_t);
#endif

#ifndef HAVE_TIMINGSAFE_BCMP
int timingsafe_bcmp(const void *b1, const void *b2, size_t n);
#endif

#ifndef HAVE_TIMINGSAFE_MEMCMP
int timingsafe_memcmp(const void *b1, const void *b2, size_t len);
#endif

#ifndef HAVE_MEMMEM
void * memmem(const void *big, size_t big_len, const void *little,
	size_t little_len);
#endif

#ifdef _WIN32
#include <errno.h>

static inline char  *
posix_strerror(int errnum)
{
	if (errnum == ECONNREFUSED) {
		return "Connection refused";
	}
	return strerror(errnum);
}

#define strerror(errnum) posix_strerror(errnum)

#endif

#endif
