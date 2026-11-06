#include <base/openpty.h>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>


int openPty(int & master_fd, int & slave_fd, const winsize & ws)
{
    int m = posix_openpt(O_RDWR | O_NOCTTY);
    if (m < 0)
        return -1;

    if (grantpt(m) != 0 || unlockpt(m) != 0)
    {
        close(m);
        return -1;
    }

    /// `ptsname` is not thread-safe on Linux, and `openPty` may be called concurrently
    /// for multiple sessions, so prefer the thread-safe `ptsname_r` where it is available.
    /// FreeBSD provides `ptsname_r` as well; macOS does not, but its `ptsname` uses
    /// thread-local storage and is therefore safe to call concurrently.
    char slave_name[64];

#if defined(OS_LINUX) || defined(OS_FREEBSD)
    /// On Linux, `ptsname_r` returns 0 on success or a positive `errno` value on failure
    /// (it does not set `errno`). Normalize so callers (`ErrnoException`) see the real reason.
    int rc = ptsname_r(m, slave_name, sizeof(slave_name));
    if (rc != 0)
    {
        if (rc > 0)
            errno = rc;
        close(m);
        return -1;
    }
#else
    const char * name = ptsname(m);
    if (name == nullptr)
    {
        close(m);
        return -1;
    }
    /// Bounded copy of the slave path into a local buffer.
    size_t i = 0;
    while (name[i] != '\0' && i + 1 < sizeof(slave_name))
    {
        slave_name[i] = name[i];
        ++i;
    }
    slave_name[i] = '\0';
#endif

    int s = open(slave_name, O_RDWR | O_NOCTTY);
    if (s < 0)
    {
        close(m);
        return -1;
    }

    if (ioctl(s, TIOCSWINSZ, &ws) != 0)
    {
        close(s);
        close(m);
        return -1;
    }

    master_fd = m;
    slave_fd = s;

    return 0;
}
