#ifndef	_TERMIOS_H
#define	_TERMIOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_pid_t
#define __NEED_struct_winsize

#include <bits/alltypes.h>

typedef unsigned char cc_t;
typedef unsigned int speed_t;
typedef unsigned int tcflag_t;

#define NCCS 32

#include <bits/termios.h>

speed_t cfgetospeed (const struct termios *);
speed_t cfgetispeed (const struct termios *);
int cfsetospeed (struct termios *, speed_t);
int cfsetispeed (struct termios *, speed_t);

int tcgetattr (int, struct termios *);
int tcsetattr (int, int, const struct termios *);

int tcgetwinsize (int, struct winsize *);
int tcsetwinsize (int, const struct winsize *);

int tcsendbreak (int, int);
int tcdrain (int);
int tcflush (int, int);
int tcflow (int, int);

pid_t tcgetsid (int);

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
void cfmakeraw(struct termios *);
int cfsetspeed(struct termios *, speed_t);
#endif

#ifdef __cplusplus
}
#endif

#endif
