#ifndef _SYS_TIMEB_H
#define _SYS_TIMEB_H
#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_time_t

#include <bits/alltypes.h>

struct timeb {
	time_t time;
	unsigned short millitm;
	short timezone, dstflag;
};

int ftime(struct timeb *);

#if _REDIR_TIME64
__REDIR(ftime, __ftime64);
#endif

#ifdef __cplusplus
}
#endif
#endif
