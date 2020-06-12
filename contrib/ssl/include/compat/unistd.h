/*
 * Public domain
 * unistd.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_UNISTD_H
#define LIBCRYPTOCOMPAT_UNISTD_H

#ifndef _MSC_VER
#include_next <unistd.h>
#else

#include <stdlib.h>
#include <io.h>
#include <process.h>

#define STDOUT_FILENO   1
#define STDERR_FILENO   2

#define R_OK    4
#define W_OK    2
#define X_OK    0
#define F_OK    0

#define access _access

unsigned int sleep(unsigned int seconds);

#endif

#ifndef HAVE_GETENTROPY
int getentropy(void *buf, size_t buflen);
#else
/*
 * Solaris 11.3 adds getentropy(2), but defines the function in sys/random.h
 */
#if defined(__sun)
#include <sys/random.h>
#endif
#endif

#ifndef HAVE_GETPAGESIZE
int getpagesize(void);
#endif

#define pledge(request, paths) 0

#ifndef HAVE_PIPE2
int pipe2(int fildes[2], int flags);
#endif

#endif
