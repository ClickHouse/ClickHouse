#ifndef	_PTY_H
#define	_PTY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <termios.h>
#include <sys/ioctl.h>

int openpty(int *, int *, char *, const struct termios *, const struct winsize *);
int forkpty(int *, char *, const struct termios *, const struct winsize *);

#ifdef __cplusplus
}
#endif

#endif
