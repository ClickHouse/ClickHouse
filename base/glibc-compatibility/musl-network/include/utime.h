#ifndef	_UTIME_H
#define	_UTIME_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_time_t

#include <bits/alltypes.h>

struct utimbuf {
	time_t actime;
	time_t modtime;
};

int utime (const char *, const struct utimbuf *);

#if _REDIR_TIME64
__REDIR(utime, __utime64);
#endif

#ifdef __cplusplus
}
#endif

#endif
