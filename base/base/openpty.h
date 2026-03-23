#pragma once

#include <fcntl.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

/**
 * Portable openpty implementation using POSIX posix_openpt/grantpt/unlockpt/ptsname.
 * Replaces the previous Linux-only musl-derived version that used /dev/ptmx and Linux-specific ioctls.
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

	char * slave_name = ptsname(m);
	if (!slave_name)
	{
		close(m);
		return -1;
	}

	int s = open(slave_name, O_RDWR | O_NOCTTY);
	if (s < 0)
	{
		close(m);
		return -1;
	}

	if (name)
	{
		/// The caller is responsible for providing a large enough buffer.
		/// We copy up to 19 chars + null to match the previous implementation's buf[20].
		size_t i = 0;
		while (slave_name[i] && i < 19)
		{
			name[i] = slave_name[i];
			++i;
		}
		name[i] = '\0';
	}

	if (tio) tcsetattr(s, TCSANOW, tio);
	if (ws) ioctl(s, TIOCSWINSZ, ws);

	*pm = m;
	*ps = s;

	return 0;
}
// NOLINTEND
