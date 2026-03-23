#pragma once

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

/**
 * Portable openpty implementation using POSIX posix_openpt/grantpt/unlockpt.
 * Uses ptsname_r on Linux (where ptsname is not thread-safe) and ptsname
 * on macOS/FreeBSD (where it uses thread-local storage and is thread-safe).
 */
// NOLINTBEGIN
inline int openpty(int *pm, int *ps, char *name, const struct termios *tio, const struct winsize *ws)
{
	int m = posix_openpt(O_RDWR | O_NOCTTY);
	if (m < 0) return -1;

	if (grantpt(m) || unlockpt(m))
	{
		close(m);
		return -1;
	}

	char slave_name_buf[64];

#ifdef __linux__
	if (ptsname_r(m, slave_name_buf, sizeof(slave_name_buf)) != 0)
	{
		close(m);
		return -1;
	}
#else
	/// On macOS and FreeBSD, ptsname uses thread-local storage and is thread-safe.
	const char * slave_name_ptr = ptsname(m);
	if (!slave_name_ptr)
	{
		close(m);
		return -1;
	}
	strncpy(slave_name_buf, slave_name_ptr, sizeof(slave_name_buf) - 1);
	slave_name_buf[sizeof(slave_name_buf) - 1] = '\0';
#endif

	int s = open(slave_name_buf, O_RDWR | O_NOCTTY);
	if (s < 0)
	{
		close(m);
		return -1;
	}

	if (name)
	{
		strncpy(name, slave_name_buf, 19);
		name[19] = '\0';
	}

	if (tio) tcsetattr(s, TCSANOW, tio);
	if (ws) ioctl(s, TIOCSWINSZ, ws);

	*pm = m;
	*ps = s;

	return 0;
}
// NOLINTEND
