#ifndef _SYS_TIMEX_H
#define _SYS_TIMEX_H

#ifdef __cplusplus
extern "C" {
#endif

#define __NEED_clockid_t

#include <bits/alltypes.h>

#include <sys/time.h>

struct ntptimeval {
	struct timeval time;
	long maxerror, esterror;
};

struct timex {
	unsigned modes;
	long offset, freq, maxerror, esterror;
	int status;
	long constant, precision, tolerance;
	struct timeval time;
	long tick, ppsfreq, jitter;
	int shift;
	long stabil, jitcnt, calcnt, errcnt, stbcnt;
	int tai;
	int __padding[11];
};

#define ADJ_OFFSET		0x0001
#define ADJ_FREQUENCY		0x0002
#define ADJ_MAXERROR		0x0004
#define ADJ_ESTERROR		0x0008
#define ADJ_STATUS		0x0010
#define ADJ_TIMECONST		0x0020
#define ADJ_TAI			0x0080
#define ADJ_SETOFFSET		0x0100
#define ADJ_MICRO		0x1000
#define ADJ_NANO		0x2000
#define ADJ_TICK		0x4000
#define ADJ_OFFSET_SINGLESHOT	0x8001
#define ADJ_OFFSET_SS_READ	0xa001

#define MOD_OFFSET	ADJ_OFFSET
#define MOD_FREQUENCY	ADJ_FREQUENCY
#define MOD_MAXERROR	ADJ_MAXERROR
#define MOD_ESTERROR	ADJ_ESTERROR
#define MOD_STATUS	ADJ_STATUS
#define MOD_TIMECONST	ADJ_TIMECONST
#define MOD_CLKB	ADJ_TICK
#define MOD_CLKA	ADJ_OFFSET_SINGLESHOT
#define MOD_TAI		ADJ_TAI
#define MOD_MICRO	ADJ_MICRO
#define MOD_NANO	ADJ_NANO

#define STA_PLL		0x0001
#define STA_PPSFREQ	0x0002
#define STA_PPSTIME	0x0004
#define STA_FLL		0x0008

#define STA_INS		0x0010
#define STA_DEL		0x0020
#define STA_UNSYNC	0x0040
#define STA_FREQHOLD	0x0080

#define STA_PPSSIGNAL	0x0100
#define STA_PPSJITTER	0x0200
#define STA_PPSWANDER	0x0400
#define STA_PPSERROR	0x0800

#define STA_CLOCKERR	0x1000
#define STA_NANO	0x2000
#define STA_MODE	0x4000
#define STA_CLK		0x8000

#define STA_RONLY (STA_PPSSIGNAL | STA_PPSJITTER | STA_PPSWANDER | \
    STA_PPSERROR | STA_CLOCKERR | STA_NANO | STA_MODE | STA_CLK)

#define TIME_OK		0
#define TIME_INS	1
#define TIME_DEL	2
#define TIME_OOP	3
#define TIME_WAIT	4
#define TIME_ERROR	5
#define TIME_BAD	TIME_ERROR

#define MAXTC		6

int adjtimex(struct timex *);
int clock_adjtime(clockid_t, struct timex *);

#if _REDIR_TIME64
__REDIR(adjtimex, __adjtimex_time64);
__REDIR(clock_adjtime, __clock_adjtime64);
#endif

#ifdef __cplusplus
}
#endif

#endif
