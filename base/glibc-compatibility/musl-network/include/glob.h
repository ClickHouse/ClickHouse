#ifndef	_GLOB_H
#define	_GLOB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_size_t

#include <bits/alltypes.h>

typedef struct {
	size_t gl_pathc;
	char **gl_pathv;
	size_t gl_offs;
	int __dummy1;
	void *__dummy2[5];
} glob_t;

int  glob(const char *__restrict, int, int (*)(const char *, int), glob_t *__restrict);
void globfree(glob_t *);

#define GLOB_ERR      0x01
#define GLOB_MARK     0x02
#define GLOB_NOSORT   0x04
#define GLOB_DOOFFS   0x08
#define GLOB_NOCHECK  0x10
#define GLOB_APPEND   0x20
#define GLOB_NOESCAPE 0x40
#define	GLOB_PERIOD   0x80

#define GLOB_TILDE       0x1000
#define GLOB_TILDE_CHECK 0x4000

#define GLOB_NOSPACE 1
#define GLOB_ABORTED 2
#define GLOB_NOMATCH 3
#define GLOB_NOSYS   4

#if defined(_LARGEFILE64_SOURCE) || defined(_GNU_SOURCE)
#define glob64 glob
#define globfree64 globfree
#define glob64_t glob_t
#endif

#ifdef __cplusplus
}
#endif

#endif
