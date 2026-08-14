#pragma once

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

/**
 * This function is taken from Musl with a few modifications.
 * Clang-tidy produces several warning, but we would love to keep the implementation
 * as close as possible to the original one.
 */
// NOLINTBEGIN
int openpty(int *pm, int *ps, char *name, const struct termios *tio, const struct winsize *ws)
{
	int m, s, n=0, cs;
	char buf[20];

	m = open("/dev/ptmx", O_RDWR|O_NOCTTY);
	if (m < 0) return -1;

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cs);

	if (ioctl(m, TIOCSPTLCK, &n) || ioctl (m, TIOCGPTN, &n))
		goto fail;

	if (!name) name = buf;
	snprintf(name, sizeof buf, "/dev/pts/%d", n);
	if ((s = open(name, O_RDWR|O_NOCTTY)) < 0)
		goto fail;

	if (tio) tcsetattr(s, TCSANOW, tio);
	if (ws) ioctl(s, TIOCSWINSZ, ws);

	*pm = m;
	*ps = s;

	pthread_setcancelstate(cs, nullptr);
	return 0;
fail:
	close(m);
	pthread_setcancelstate(cs, nullptr);
	return -1;
}
// NOLINTEND
